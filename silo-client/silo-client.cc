extern "C" {
#include <base/time.h>
#include <base/log.h>
#include <net/ip.h>
#include <runtime/smalloc.h>
#include <unistd.h>
#include <breakwater/breakwater.h>
#include <breakwater/seda.h>
#include <breakwater/dagor.h>
#include <breakwater/nocontrol.h>
}

#include "cc/net.h"
#include "cc/runtime.h"
#include "cc/sync.h"
#include "cc/thread.h"
#include "cc/timer.h"
#include "proto.h"
#include "breakwater/rpc++.h"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <ctime>
std::time_t timex;

barrier_t barrier;

constexpr uint16_t kBarrierPort = 41;
constexpr int kValueLen = 5;

const struct crpc_ops *crpc_ops;
const struct srpc_ops *srpc_ops;

namespace {

using namespace std::chrono;
using sec = duration<double, std::micro>;

// <- ARGUMENTS FOR EXPERIMENT ->
// the number of worker threads to spawn.
int threads;
// the remote UDP address of the server.
netaddr raddr, master;
int wtype;
// RPC service level objective (in us)
int slo;
// maximum key index
int max_key_idx;

std::ofstream json_out;
std::ofstream csv_out;

int total_agents = 1;
// Total duration of the experiment in us
constexpr uint64_t kWarmUpTime = 4000000; // was 2000000
constexpr uint64_t kExperimentTime = 8000000; // was 4000000
// RTT
constexpr uint64_t kRTT = 10;

std::vector<double> offered_loads;
double offered_load;

/* server-side stat */
constexpr uint64_t kRPCSStatPort = 8002;
constexpr uint64_t kRPCSStatMagic = 0xDEADBEEF;
struct sstat_raw {
  uint64_t idle;
  uint64_t busy;
  unsigned int num_cores;
  unsigned int max_cores;
  uint64_t winu_rx;
  uint64_t winu_tx;
  uint64_t win_tx;
  uint64_t req_rx;
  uint64_t resp_tx;
};

constexpr uint64_t kShenangoStatPort = 40;
constexpr uint64_t kShenangoStatMagic = 0xDEADBEEF;
struct shstat_raw {
  uint64_t rx_pkts;
  uint64_t tx_pkts;
  uint64_t rx_bytes;
  uint64_t tx_bytes;
  uint64_t drops;
  uint64_t rx_tcp_ooo;
};

struct sstat {
  double cpu_usage;
  double rx_pps;
  double tx_pps;
  double rx_bps;
  double tx_bps;
  double rx_drops_pps;
  double rx_ooo_pps;
  double winu_rx_pps;
  double winu_tx_pps;
  double win_tx_wps;
  double req_rx_pps;
  double resp_tx_pps;
};

/* client-side stat */
struct cstat_raw {
  double offered_rps;
  double rps;
  double goodput;
  double min_percli_tput;
  double max_percli_tput;
  uint64_t winu_rx;
  uint64_t winu_tx;
  uint64_t resp_rx;
  uint64_t req_tx;
  uint64_t win_expired;
  uint64_t req_dropped;
};

struct cstat {
  double offered_rps;
  double rps;
  double goodput;
  double min_percli_tput;
  double max_percli_tput;
  double winu_rx_pps;
  double winu_tx_pps;
  double resp_rx_pps;
  double req_tx_pps;
  double win_expired_wps;
  double req_dropped_rps;
};

struct work_unit {
  double start_us, duration_us, latency_us;
  int hash;
  uint64_t window;
  uint64_t tsc;
  uint32_t cpu;
  uint64_t server_queue;
  uint64_t client_queue;
};

class NetBarrier {
  public:
  static constexpr uint64_t npara = 10;
  NetBarrier(int npeers) {
    threads /= total_agents;

    is_leader_ = true;
    std::unique_ptr<rt::TcpQueue> q(
        rt::TcpQueue::Listen({0, kBarrierPort}, 4096));
    aggregator_ = std::move(std::unique_ptr<rt::TcpQueue>(
        rt::TcpQueue::Listen({0, kBarrierPort + 1}, 4096)));
    for (int i = 0; i < npeers; i++) {
      rt::TcpConn *c = q->Accept();
      if (c == nullptr) panic("couldn't accept a connection");
      conns.emplace_back(c);
      BUG_ON(c->WriteFull(&threads, sizeof(threads)) <= 0);
      BUG_ON(c->WriteFull(&raddr, sizeof(raddr)) <= 0);
      //BUG_ON(c->WriteFull(&wtype, sizeof(wtype)) <= 0);
      BUG_ON(c->WriteFull(&total_agents, sizeof(total_agents)) <= 0);
      BUG_ON(c->WriteFull(&slo, sizeof(slo)) <= 0);
      //BUG_ON(c->WriteFull(&max_key_idx, sizeof(max_key_idx)) <= 0);
      BUG_ON(c->WriteFull(&offered_load, sizeof(offered_load)) <= 0);
      for (size_t j = 0; j < npara; j++) {
        rt::TcpConn *c = aggregator_->Accept();
        if (c == nullptr) panic("couldn't accept a connection");
        agg_conns_.emplace_back(c);
      }
    }
  }

  NetBarrier(netaddr leader) {
    auto c = rt::TcpConn::Dial({0, 0}, {leader.ip, kBarrierPort});
    if (c == nullptr) panic("barrier");
    conns.emplace_back(c);
    is_leader_ = false;
    BUG_ON(c->ReadFull(&threads, sizeof(threads)) <= 0);
    BUG_ON(c->ReadFull(&raddr, sizeof(raddr)) <= 0);
    //BUG_ON(c->ReadFull(&wtype, sizeof(wtype)) <= 0);
    BUG_ON(c->ReadFull(&total_agents, sizeof(total_agents)) <= 0);
    BUG_ON(c->ReadFull(&slo, sizeof(slo)) <= 0);
    //BUG_ON(c->ReadFull(&max_key_idx, sizeof(max_key_idx)) <= 0);
    BUG_ON(c->ReadFull(&offered_load, sizeof(offered_load)) <= 0);
    for (size_t i = 0; i < npara; i++) {
      auto c = rt::TcpConn::Dial({0, 0}, {master.ip, kBarrierPort + 1});
      BUG_ON(c == nullptr);
      agg_conns_.emplace_back(c);
    }
  }

  bool Barrier() {
    char buf[1];
    if (is_leader_) {
      for (auto &c : conns) {
        if (c->ReadFull(buf, 1) != 1) return false;
      }
      for (auto &c : conns) {
        if (c->WriteFull(buf, 1) != 1) return false;
      }
    } else {
      if (conns[0]->WriteFull(buf, 1) != 1) return false;
      if (conns[0]->ReadFull(buf, 1) != 1) return false;
    }
    return true;
  }

  bool StartExperiment() { return Barrier(); }

  bool EndExperiment(std::vector<Packet> &w, struct cstat_raw *csr) {
    if (is_leader_) {
      for (auto &c : conns) {
    struct cstat_raw rem_csr;
    BUG_ON(c->ReadFull(&rem_csr, sizeof(rem_csr)) <= 0);
    csr->offered_rps += rem_csr.offered_rps;
    csr->rps += rem_csr.rps;
    csr->goodput += rem_csr.goodput;
    csr->min_percli_tput = MIN(rem_csr.min_percli_tput,
                    csr->min_percli_tput);
    csr->max_percli_tput = MAX(rem_csr.max_percli_tput,
                    csr->max_percli_tput);
    csr->winu_rx += rem_csr.winu_rx;
    csr->winu_tx += rem_csr.winu_tx;
    csr->resp_rx += rem_csr.resp_rx;
    csr->req_tx += rem_csr.req_tx;
    csr->win_expired += rem_csr.win_expired;
    csr->req_dropped += rem_csr.req_dropped;
      }
    } else {
      BUG_ON(conns[0]->WriteFull(csr, sizeof(*csr)) <= 0);
    }
    GatherSamples(w);
    BUG_ON(!Barrier());
    return is_leader_;
  }

  bool IsLeader() {
    return is_leader_;
  }

  private:
  std::vector<std::unique_ptr<rt::TcpConn>> conns;
  std::unique_ptr<rt::TcpQueue> aggregator_;
  std::vector<std::unique_ptr<rt::TcpConn>> agg_conns_;
  bool is_leader_;

  void GatherSamples(std::vector<Packet> &w) {
    std::vector<rt::Thread> th;
    if (is_leader_) {
      std::unique_ptr<std::vector<Packet>> samples[agg_conns_.size()];
      for (size_t i = 0; i < agg_conns_.size(); ++i) {
        th.emplace_back(rt::Thread([&, i] {
          size_t nelem;
          BUG_ON(agg_conns_[i]->ReadFull(&nelem, sizeof(nelem)) <= 0);

      if (likely(nelem > 0)) {
            Packet *wunits = new Packet[nelem];
            BUG_ON(agg_conns_[i]->ReadFull(wunits, sizeof(Packet) * nelem) <=
                    0);
            std::vector<Packet> v(wunits, wunits + nelem);
            delete[] wunits;

            samples[i].reset(new std::vector<Packet>(std::move(v)));
      } else {
        samples[i].reset(new std::vector<Packet>());
      }
        }));
      }

      for (auto &t : th) t.Join();
      for (size_t i = 0; i < agg_conns_.size(); ++i) {
        auto &v = *samples[i];
        w.insert(w.end(), v.begin(), v.end());
      }
    } else {
      for (size_t i = 0; i < agg_conns_.size(); ++i) {
        th.emplace_back(rt::Thread([&, i] {
          size_t elems = w.size() / npara;
          Packet *start = w.data() + elems * i;
          if (i == npara - 1) elems += w.size() % npara;
          BUG_ON(agg_conns_[i]->WriteFull(&elems, sizeof(elems)) <= 0);
      if (likely(elems > 0))
        BUG_ON(agg_conns_[i]->WriteFull(start, sizeof(Packet) * elems)
                <= 0);
        }));
      }
      for (auto &t : th) t.Join();
    }
  }
};

static NetBarrier *b;

sstat_raw ReadRPCSStat() {
  std::unique_ptr<rt::TcpConn> c(
      rt::TcpConn::Dial({0, 0}, {raddr.ip, kRPCSStatPort}));
  uint64_t magic = hton64(kRPCSStatMagic);
  ssize_t ret = c->WriteFull(&magic, sizeof(magic));
  if (ret != static_cast<ssize_t>(sizeof(magic)))
  panic("sstat request failed, ret = %ld", ret);
  sstat_raw u;
  ret = c->ReadFull(&u, sizeof(u));
  if (ret != static_cast<ssize_t>(sizeof(u)))
    panic("sstat response failed, ret = %ld", ret);
  return sstat_raw{u.idle, u.busy, u.num_cores, u.max_cores, u.winu_rx,
                    u.winu_tx, u.win_tx, u.req_rx, u.resp_tx};
}

shstat_raw ReadShenangoStat() {
  char *buf_;
  std::string buf;
  std::map<std::string, uint64_t> smap;
  std::unique_ptr<rt::TcpConn> c(
      rt::TcpConn::Dial({0,0}, {raddr.ip, kShenangoStatPort}));
  uint64_t magic = hton64(kShenangoStatMagic);
  ssize_t ret = c->WriteFull(&magic, sizeof(magic));
  if (ret != static_cast<ssize_t>(sizeof(magic)))
    panic("Shenango stat request failed, ret = %ld", ret);

  size_t resp_len;
  ret = c->ReadFull(&resp_len, sizeof(resp_len));
  if (ret != static_cast<ssize_t>(sizeof(resp_len)))
    panic("Shenango stat response failed, ret = %ld", ret);

  buf_ = (char *)malloc(resp_len);

  ret = c->ReadFull(buf_, resp_len);
  if (ret != static_cast<ssize_t>(resp_len))
    panic("Shenango stat response failed, ret = %ld", ret);

  buf = std::string(buf_);

  size_t pos_com = 0;
  size_t pos_col = 0;
  std::string token;
  std::string key;
  uint64_t value;

  while ((pos_com = buf.find(",")) != std::string::npos) {
    token = buf.substr(0, pos_com);
    pos_col = token.find(":");
    if (pos_col == std::string::npos)
      continue;

    key = token.substr(0, pos_col);
    value = std::stoull(token.substr(pos_col+1, pos_com));

    smap[key] = value;

    buf.erase(0, pos_com + 1);
  }

  free(buf_);

  return shstat_raw{smap["rx_packets"], smap["tx_packets"], smap["rx_bytes"],
                smap["tx_bytes"], smap["drops"], smap["rx_tcp_out_of_order"]};
}

constexpr uint64_t kNetbenchPort = 8001;

// The maximum lateness to tolerate before dropping egress samples.
constexpr uint64_t kMaxCatchUpUS = 5;

template <class Arrival>
std::vector<Packet> GenerateWork(Arrival a, double cur_us,
                                    double last_us) {
  std::vector<Packet> w;
  while (true) {
    cur_us += a();
    if (cur_us > last_us) break;
    w.emplace_back(Packet{0, 0, cur_us, 0, 0, 0, 0, 0});
  }

  return w;
}

void GenerateRandomString(char *buffer, int len, uint64_t hash) {
  int i;
  uint64_t tmp_hash = hash;

  for(i = 0; i < len; ++i) {
    buffer[i] = (tmp_hash % 94) + 33;
    tmp_hash = (tmp_hash >> 1);
  }
}

std::vector<Packet> ClientWorker(
    rpc::RpcClient *c, rt::WaitGroup *starter, rt::WaitGroup *starter2,
    std::function<std::vector<Packet>()> wf) {
  std::vector<Packet> w(wf());
  std::vector<uint64_t> timings;
  timings.reserve(w.size());

  // Start the receiver thread.
  auto th = rt::Thread([&] {
    char resp[4096];
    struct Payload *pld;

    while (true) {
      ssize_t ret = c->Recv(resp, 4096, 0, nullptr);


      pld = reinterpret_cast<struct Payload *>(resp);
      ntoh(pld);

      if (ret != static_cast<ssize_t>(sizeof(Payload))) {
        if (ret == 0 || ret < 0) break;
        panic("read failed, ret = %ld", ret);
      }

      uint64_t now = microtime();
      uint64_t idx = pld->index;

      w[idx].completion_server_tsc = pld->randomness;
      w[idx].completion_time = now - timings[idx];
      w[idx].latency_us = now - timings[idx];
      w[idx].window = c->Credit();
    }
  });

  // Synchronized start of load generation.
  starter->Done();
  starter2->Wait();

  barrier();
  auto expstart = steady_clock::now();
  barrier();

  auto wsize = w.size();
  char buf[4096];
  struct Payload *pld;
  unsigned int i;

  for (i = 0; i < wsize; ++i) {
    barrier();
    auto now = steady_clock::now();
    barrier();
    if (duration_cast<sec>(now - expstart).count() < w[i].target_start) { // w[i].target_start instead of w[i].start_us
      rt::Sleep(w[i].target_start - duration_cast<sec>(now - expstart).count());
    }
    if (duration_cast<sec>(now - expstart).count() - w[i].target_start >
        kMaxCatchUpUS)
      continue;


    timings[i] = microtime();

    pld = reinterpret_cast<struct Payload *>(buf);
    pld->work_iterations = 0;
    pld->index = i;
    pld->randomness = w[i].randomness;

    hton(pld);
    // Send an RPC request.
    ssize_t ret = c->Send(pld, sizeof(Payload), i, nullptr); // Note, could be buggy
    if (ret == -ENOBUFS) continue;
    if (ret != static_cast<ssize_t>(sizeof(Payload)))
      panic("write failed, ret = %ld", ret);
  }

  // rt::Sleep(1 * rt::kSeconds);
  rt::Sleep((int)(kRTT + 2)); // shuld sleep for RTT + 10?
  BUG_ON(c->Shutdown(SHUT_RDWR));
  th.Join();

  return w;
}

std::vector<Packet> RunExperiment(
    int threads, struct cstat_raw *csr, struct sstat *ss, double *elapsed,
    std::function<std::vector<Packet>()> wf) {
  // Create one TCP connection per thread.
  std::vector<std::unique_ptr<rpc::RpcClient>> conns;
  for (int i = 0; i < threads; ++i) {
    struct rpc_session_info info = {.session_type = 0}; // from protego version
    // passing nullptrs for drop handlers, shouldn't be an issue (checks in bw_client.c)
    std::unique_ptr<rpc::RpcClient> outc(rpc::RpcClient::Dial(raddr, i+1, nullptr, nullptr, &info));
    if (unlikely(outc == nullptr)) panic("couldn't connect to raddr.");
    conns.emplace_back(std::move(outc));
  }

  // Launch a worker thread for each connection.
  rt::WaitGroup starter(threads);
  rt::WaitGroup starter2(1);

  std::vector<rt::Thread> th;
  std::unique_ptr<std::vector<Packet>> samples[threads];
  for (int i = 0; i < threads; ++i) {
    th.emplace_back(rt::Thread([&, i] {
      auto v = ClientWorker(conns[i].get(), &starter, &starter2, wf);
      samples[i].reset(new std::vector<Packet>(std::move(v)));
    }));
  }

  // Give the workers time to initialize, then start recording.
  starter.Wait();
  if (b && !b->StartExperiment()) {
    exit(0);
  }
  starter2.Done();

  // |--- start experiment duration timing ---|
  barrier();
  timex = std::time(nullptr);
  auto start = steady_clock::now();
  barrier();
  sstat_raw s1, s2;
  shstat_raw sh1, sh2;

  if (!b || b->IsLeader()) {
    s1 = ReadRPCSStat();
    sh1 = ReadShenangoStat();
  }

  // Wait for the workers to finish.
  for (auto &t : th) t.Join();

  // |--- end experiment duration timing ---|
  barrier();
  auto finish = steady_clock::now();
  barrier();

  if (!b || b->IsLeader()) {
    s2 = ReadRPCSStat();
    sh2 = ReadShenangoStat();
  }

  // Force the connections to close.
  for (auto &c : conns) c->Abort();

  double elapsed_ = duration_cast<sec>(finish - start).count();
  elapsed_ -= kWarmUpTime;

  // Aggregate client stats
  if (csr) {
    for (auto &c : conns) {
      csr->winu_rx += c->StatEcreditRx(); // was StatWinuRx()
      csr->winu_tx += c->StatCupdateTx(); // was StatWinuTx()
      csr->resp_rx += c->StatRespRx(); // unchanged
      csr->req_tx += c->StatReqTx(); // unchanged
      csr->win_expired += c->StatCreditExpired(); // was StatWinExpired()
      csr->req_dropped += c->StatReqDropped(); // unchanged
      c->Close();
    }
  }

  // Aggregate all the samples together.
  std::vector<Packet> w;
  double min_throughput = 0.0;
  double max_throughput = 0.0;
  uint64_t good_resps = 0;
  uint64_t offered = 0;

  for (int i = 0; i < threads; ++i) {
    auto &v = *samples[i];
    double throughput;
    int slo_success;

    offered += v.size();
    // Remove requests that did not complete.
    v.erase(std::remove_if(v.begin(), v.end(),
                [](const Packet &s) {return (s.completion_time == 0 ||
                        (s.target_start + s.completion_time) < kWarmUpTime);}),
        v.end());
    slo_success = std::count_if(v.begin(), v.end(), [](const Packet &s) {
                return s.completion_time < slo;});
    throughput = static_cast<double>(v.size()) / elapsed_ * 1000000;

    good_resps += slo_success;
    if (i == 0) {
      min_throughput = throughput;
      max_throughput = throughput;
    } else {
      min_throughput = MIN(throughput, min_throughput);
      max_throughput = MAX(throughput, max_throughput);
    }

    w.insert(w.end(), v.begin(), v.end());
  }

   // Report results.
  if (csr) {
    csr->offered_rps = static_cast<double>(offered) / elapsed_ * 1000000;
    csr->offered_rps *= static_cast<double>(kExperimentTime - kWarmUpTime) / kExperimentTime;
    csr->rps = static_cast<double>(w.size()) / elapsed_ * 1000000;
    csr->goodput = static_cast<double>(good_resps) / elapsed_ * 1000000;
    csr->min_percli_tput = min_throughput;
    csr->max_percli_tput = max_throughput;
  }

  if ((!b || b->IsLeader()) && ss) {
    uint64_t idle = s2.idle - s1.idle;
    uint64_t busy = s2.busy - s1.busy;
    ss->cpu_usage = 0; // not supported yet
    // ss->cpu_usage = static_cast<double>(busy) / static_cast<double>(idle + busy);

    // ss->cpu_usage = (ss->cpu_usage - 1 / static_cast<double>(s1.max_cores)) /
// 	    (static_cast<double>(s1.num_cores) / static_cast<double>(s1.max_cores));

    uint64_t winu_rx_pkts = s2.winu_rx - s1.winu_rx;
    uint64_t winu_tx_pkts = s2.winu_tx - s1.winu_tx;
    uint64_t win_tx_wins = s2.win_tx - s1.win_tx;
    uint64_t req_rx_pkts = s2.req_rx - s1.req_rx;
    uint64_t resp_tx_pkts = s2.resp_tx - s1.resp_tx;
    ss->winu_rx_pps = static_cast<double>(winu_rx_pkts) / elapsed_ * 1000000;
    ss->winu_tx_pps = static_cast<double>(winu_tx_pkts) / elapsed_ * 1000000;
    ss->win_tx_wps = static_cast<double>(win_tx_wins) / elapsed_ * 1000000;
    ss->req_rx_pps = static_cast<double>(req_rx_pkts) / elapsed_ * 1000000;
    ss->resp_tx_pps = static_cast<double>(resp_tx_pkts) / elapsed_ * 1000000;

    uint64_t rx_pkts = sh2.rx_pkts - sh1.rx_pkts;
    uint64_t tx_pkts = sh2.tx_pkts - sh1.tx_pkts;
    uint64_t rx_bytes = sh2.rx_bytes - sh1.rx_bytes;
    uint64_t tx_bytes = sh2.tx_bytes - sh1.tx_bytes;
    uint64_t drops = sh2.drops - sh1.drops;
    uint64_t rx_tcp_ooo = sh2.rx_tcp_ooo - sh1.rx_tcp_ooo;
    ss->rx_pps = static_cast<double>(rx_pkts) / elapsed_ * 1000000;
    ss->tx_pps = static_cast<double>(tx_pkts) / elapsed_ * 1000000;
    ss->rx_bps = static_cast<double>(rx_bytes) / elapsed_ * 8000000;
    ss->tx_bps = static_cast<double>(tx_bytes) / elapsed_ * 8000000;
    ss->rx_drops_pps = static_cast<double>(drops) / elapsed_ * 1000000;
    ss->rx_ooo_pps = static_cast<double>(rx_tcp_ooo) / elapsed_ * 1000000;
  }

  *elapsed = elapsed_;

  return w;
}

void PrintHeader(std::ostream& os) {
  os << "num_threads," << "offered_load," << "throughput," << "goodput," << "cpu," << "min,"
      << "mean," << "p50," << "p90," << "p99," << "p999," << "p9999," << "max,"
      << "lmin," << "lmean," << "lp50," << "lp90," << "lp99," << "lp999," << "lp9999,"
      << "lmax," << "p1_win," << "mean_win," << "p99_win," << "p1_q," << "mean_q,"
      << "p99_q," << "server:rx_pps," << "server:tx_pps," << "server:rx_bps,"
      << "server:tx_bps," << "server:rx_drops_pps," << "server:rx_ooo_pps," << "server:winu_rx_pps,"
      << "server:winu_tx_pps," << "server:win_tx_wps," << "server:req_rx_pps,"
      << "server:resp_tx_pps," << "client:min_tput," << "client:max_tput,"
      << "client:winu_rx_pps," << "client:winu_tx_pps," << "client:resp_rx_pps,"
      << "client:req_tx_pps," << "client:win_expired_wps," << "client:req_dropped_rps"
      << std::endl;
}

void PrintStatResults(std::vector<Packet> w, struct cstat *cs,
              struct sstat *ss) {
  if (w.size() == 0) {
    std::cout << std::setprecision(4) << std::fixed << threads * total_agents
    << "," << cs->offered_rps << "," << "-" << std::endl;
    return;
  }

  std::sort(w.begin(), w.end(), [](const Packet &s1, const Packet &s2) {
    return s1.completion_time < s2.completion_time;
  });
  double sum = std::accumulate(
      w.begin(), w.end(), 0.0,
      [](double s, const Packet &c) { return s + c.completion_time; });
  double mean = sum / w.size();
  double count = static_cast<double>(w.size());
  double p50 = w[count * 0.5].completion_time;
  double p90 = w[count * 0.9].completion_time;
  double p99 = w[count * 0.99].completion_time;
  double p999 = w[count * 0.999].completion_time;
  double p9999 = w[count * 0.9999].completion_time;
  double min = w[0].completion_time;
  double max = w[w.size() - 1].completion_time;

  std::sort(w.begin(), w.end(), [](const Packet &s1, const Packet &s2) {
    return s1.latency_us < s2.latency_us;
  });
  double lsum = std::accumulate(
      w.begin(), w.end(), 0.0,
      [](double s, const Packet &c) { return s + c.latency_us; });
  double lmean = lsum / w.size();
  double lp50 = w[count * 0.5].latency_us;
  double lp90 = w[count * 0.9].latency_us;
  double lp99 = w[count * 0.99].latency_us;
  double lp999 = w[count * 0.999].latency_us;
  double lp9999 = w[count * 0.9999].latency_us;
  double lmin = w[0].latency_us;
  double lmax = w[w.size() - 1].latency_us;

  std::sort(w.begin(), w.end(), [](const Packet &s1, const Packet &s2) {
    return s1.window < s2.window;
  });
  double sum_win = std::accumulate(
      w.begin(), w.end(), 0.0,
      [](double s, const Packet &c) { return s + c.window; });
  double mean_win = sum_win / w.size();
  double p1_win = w[count * 0.01].window;
  double p99_win = w[count * 0.99].window;

  std::sort(w.begin(), w.end(), [](const Packet &s1, const Packet &s2) {
    return s1.server_queue < s2.server_queue;
  });
  double sum_que = std::accumulate(
      w.begin(), w.end(), 0.0,
      [](double s, const Packet &c) { return s + c.server_queue; });
  double mean_que = sum_que / w.size();
  double p1_que = w[count * 0.01].server_queue;
  double p99_que = w[count * 0.99].server_queue;

  double sum_cque = std::accumulate(
      w.begin(), w.end(), 0.0,
      [](double s, const Packet &c) {return s + c.client_queue; });
  double mean_cque = sum_cque / w.size();

  std::cout << std::setprecision(4) << std::fixed << threads * total_agents << ","
      << cs->offered_rps << "," << cs->rps << "," << cs->goodput << "," << ss->cpu_usage << ","
      << min << "," << mean << "," << p50 << "," << p90 << "," << p99 << ","
      << p999 << "," << p9999 << "," << max << "," << lmin << "," << lmean << ","
      << lp50 << "," << lp90 << "," << lp99 << "," << lp999 << ","
      << lp9999 << "," << lmax << "," << p1_win << ","
      << mean_win << "," << p99_win << "," << p1_que << "," << mean_que << ","
      << p99_que << "," << ss->rx_pps << "," << ss->tx_pps << ","
      << ss->rx_bps << "," << ss->tx_bps << "," << ss->rx_drops_pps << ","
      << ss->rx_ooo_pps << "," << ss->winu_rx_pps << "," << ss->winu_tx_pps << ","
      << ss->win_tx_wps << "," << ss->req_rx_pps << "," << ss->resp_tx_pps << ","
      << cs->min_percli_tput << "," << cs->max_percli_tput << ","
      << mean_cque << "," << cs->winu_rx_pps << "," << cs->resp_rx_pps << ","
      << cs->req_tx_pps << "," << cs->win_expired_wps << ","
      << cs->req_dropped_rps << std::endl;

  csv_out << std::setprecision(4) << std::fixed << threads * total_agents << ","
      << cs->offered_rps << "," << cs->rps << "," << cs->goodput << "," << ss->cpu_usage << ","
      << min << "," << mean << "," << p50 << "," << p90 << "," << p99 << ","
      << p999 << "," << p9999 << "," << max << "," << lmin << "," << lmean << ","
      << lp50 << "," << lp90 << "," << lp99 << "," << lp999 << ","
      << lp9999 << "," << lmax << "," << p1_win << ","
      << mean_win << "," << p99_win << "," << p1_que << "," << mean_que << ","
      << p99_que << "," << ss->rx_pps << "," << ss->tx_pps << ","
      << ss->rx_bps << "," << ss->tx_bps << "," << ss->rx_drops_pps << ","
      << ss->rx_ooo_pps << "," << ss->winu_rx_pps << "," << ss->winu_tx_pps << ","
      << ss->win_tx_wps << "," << ss->req_rx_pps << "," << ss->resp_tx_pps << ","
      << cs->min_percli_tput << "," << cs->max_percli_tput << ","
      << mean_cque << "," << cs->winu_rx_pps << "," << cs->resp_rx_pps << ","
      << cs->req_tx_pps << "," << cs->win_expired_wps << ","
      << cs->req_dropped_rps << std::endl << std::flush;

  json_out << "{"
        << "\"num_threads\":" << threads * total_agents << ","
        << "\"offered_load\":" << cs->offered_rps << ","
        << "\"throughput\":" << cs->rps << ","
        << "\"goodput\":" << cs->goodput << ","
        << "\"cpu\":" << ss->cpu_usage << ","
        << "\"min\":" << min << ","
        << "\"mean\":" << mean << ","
        << "\"p50\":" << p50 << ","
        << "\"p90\":" << p90 << ","
        << "\"p99\":" << p99 << ","
        << "\"p999\":" << p999 << ","
        << "\"p9999\":" << p9999 << ","
        << "\"max\":" << max << ","
        << "\"lmin\":" << lmin << ","
        << "\"lmean\":" << lmean << ","
        << "\"lp50\":" << lp50 << ","
        << "\"lp90\":" << lp90 << ","
        << "\"lp99\":" << lp99 << ","
        << "\"lp999\":" << lp999 << ","
        << "\"lp9999\":" << lp9999 << ","
        << "\"lmax\":" << lmax << ","
        << "\"p1_win\":" << p1_win << ","
        << "\"mean_win\":" << mean_win << ","
        << "\"p99_win\":" << p99_win << ","
        << "\"p1_q\":" << p1_que << ","
        << "\"mean_q\":" << mean_que << ","
        << "\"p99_q\":" << p99_que << ","
        << "\"server:rx_pps\":" << ss->rx_pps << ","
        << "\"server:tx_pps\":" << ss->tx_pps << ","
        << "\"server:rx_bps\":" << ss->rx_bps << ","
        << "\"server:tx_bps\":" << ss->tx_bps << ","
        << "\"server:rx_drops_pps\":" << ss->rx_drops_pps << ","
        << "\"server:rx_ooo_pps\":" << ss->rx_ooo_pps << ","
        << "\"server:winu_rx_pps\":" << ss->winu_rx_pps << ","
        << "\"server:winu_tx_pps\":" << ss->winu_tx_pps << ","
        << "\"server:win_tx_wps\":" << ss->win_tx_wps << ","
        << "\"server:req_rx_pps\":" << ss->req_rx_pps << ","
        << "\"server:resp_tx_pps\":" << ss->resp_tx_pps << ","
        << "\"client:min_tput\":" << cs->min_percli_tput << ","
        << "\"client:max_tput\":" << cs->max_percli_tput << ","
        << "\"client:mean_q\":" << mean_cque << ","
        << "\"client:winu_rx_pps\":" << cs->winu_rx_pps << ","
        << "\"client:winu_tx_pps\":" << cs->winu_tx_pps << ","
        << "\"client:resp_rx_pps\":" << cs->resp_rx_pps << ","
        << "\"client:req_tx_pps\":" << cs->req_tx_pps << ","
        << "\"client:win_expired_wps\":" << cs->win_expired_wps << ","
        << "\"client:req_dropped_rps\":" << cs->req_dropped_rps
        << "}," << std::endl << std::flush;
}

void SteadyStateExperiment(int threads, double offered_rps) {
  struct sstat ss;
  struct cstat_raw csr;
  struct cstat cs;
  double elapsed;

  memset(&csr, 0, sizeof(csr));
  std::vector<Packet> w = RunExperiment(threads, &csr, &ss, &elapsed,
                        [=] {
    std::mt19937 rg(rand());
    std::exponential_distribution<double> rd(
        1.0 / (1000000.0 / (offered_rps / static_cast<double>(threads))));
    return GenerateWork(std::bind(rd, rg), 0, kExperimentTime);
  });

  if (b) {
    if (!b->EndExperiment(w, &csr))
      return;
  }

  cs = cstat{csr.offered_rps,
          csr.rps,
          csr.goodput,
          csr.min_percli_tput,
          csr.max_percli_tput,
          static_cast<double>(csr.winu_rx) / elapsed * 1000000,
          static_cast<double>(csr.winu_tx) / elapsed * 1000000,
          static_cast<double>(csr.resp_rx) / elapsed * 1000000,
          static_cast<double>(csr.req_tx) / elapsed * 1000000,
          static_cast<double>(csr.win_expired) / elapsed * 1000000,
          static_cast<double>(csr.req_dropped) / elapsed * 1000000};

  // Print the results.
  PrintStatResults(w, &cs, &ss);
}

int StringToAddr(const char *str, uint32_t *addr) {
  uint8_t a, b, c, d;

  if (sscanf(str, "%hhu.%hhu.%hhu.%hhu", &a, &b, &c, &d) != 4) return -EINVAL;

  *addr = MAKE_IP_ADDR(a, b, c, d);
  return 0;
}

void calculate_rates() {
  offered_loads.push_back(offered_load / (double)total_agents);
}

void AgentHandler(void *arg) {
  master.port = kBarrierPort;
  b = new NetBarrier(master);
  BUG_ON(!b);

  calculate_rates();

  for (double i : offered_loads) {
    SteadyStateExperiment(threads, i);
  }
}

void ClientHandler(void *arg) {
  int pos;

  if (total_agents > 1) {
    b = new NetBarrier(total_agents - 1);
    BUG_ON(!b);
  }

  calculate_rates();

  json_out.open("output.json");
  csv_out.open("output.csv", std::fstream::out | std::fstream::app);

  json_out << "[";

  /* Print Header */
//  PrintHeader(csv_out);
  PrintHeader(std::cout);

  for (double i : offered_loads) {
    SteadyStateExperiment(threads, i);
  }

  pos = json_out.tellp();
  json_out.seekp(pos-2);
  json_out << "]";
  json_out.close();
  csv_out.close();
}

}  // anonymous namespace

int main(int argc, char *argv[]) {
  int ret;

  if (argc < 4) {
    std::cerr << "usage: [alg] [cfg_file] [cmd] ..." << std::endl;
    return -EINVAL;
  }

  std::string olcalg = argv[1];
  if (olcalg.compare("breakwater") == 0) {
    crpc_ops = &cbw_ops;
    srpc_ops = &sbw_ops;
  } else if (olcalg.compare("dagor") == 0) {
    crpc_ops = &cdg_ops;
    srpc_ops = &sdg_ops;
  } else if (olcalg.compare("seda") == 0) {
    crpc_ops = &csd_ops;
    srpc_ops = &ssd_ops;
  } else if (olcalg.compare("nocontrol") == 0) {
    crpc_ops = &cnc_ops;
    srpc_ops = &snc_ops;
  } else {
    std::cerr << "Invalid overload control algorithm: " << olcalg << std::endl;
    return -EINVAL;
  }

  std::string cmd = argv[3];
  if (cmd.compare("agent") == 0) {
    if (argc < 5 || StringToAddr(argv[4], &master.ip)) {
      std::cerr << "usage: [alg] [cfg_file] agent [ip_address]"
          << std::endl;
      return -EINVAL;
    }

    ret = runtime_init(argv[2], AgentHandler, NULL);
    if (ret) {
      printf("failed to start runtime\n");
      return ret;
    }
  } else if (cmd.compare("client") != 0) {
    std::cerr << "invalid command: " << cmd << std::endl;
    return -EINVAL;
  }

  if (argc < 9) {
    std::cerr << "usage: [alg] [cfg_file] client [#threads] [remote_ip]"
	    << " [slo] [npeers] [offered_load]"
	    << std::endl;
    return -EINVAL;
  }

  threads = std::stoi(argv[4], nullptr, 0);

  ret = StringToAddr(argv[5], &raddr.ip);
  if (ret) return -EINVAL;
  raddr.port = kNetbenchPort;
  slo = std::stoi(argv[6], nullptr, 0);
  total_agents += std::stoi(argv[7], nullptr, 0);
  offered_load = std::stod(argv[8], nullptr);
  ret = runtime_init(argv[2], ClientHandler, NULL);
  if (ret) {
    printf("failed to start runtime\n");
    return ret;
  }

  return 0;
}
