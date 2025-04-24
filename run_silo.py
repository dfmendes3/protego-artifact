#!/usr/bin/env python3

import paramiko
import os
from time import sleep
from util import *
from config_remote import *

################################
### Experiemnt Configuration ###
################################
"""

for timeseries, might just have to run this one at a time

"""
# Server overload algorithm (breakwater, seda, dagor, nocontrol)

SPIN_SERVER = False
POLICY = "coresync"

if POLICY.lower() == "coresync":
    SCHEDULER = "ias"
    OVERLOAD_ALG = "breakwater"
    NUM_CORES_GUARANTEED = 0
    UTILIZATION_RANGE = False
    SBW_CORE_PARK_TARGET = 0.1
    CORE_CREDIT_RATIO = 10
    print(f"{POLICY} has been chosen.")
elif POLICY.lower() == "caladan":
    SCHEDULER = "ias"
    OVERLOAD_ALG = "breakwater"
    NUM_CORES_GUARANTEED = 0
    UTILIZATION_RANGE = False
    print(f"{POLICY} has been chosen.")
elif POLICY.lower() == "shenango" or POLICY.lower() == "simple":
    SCHEDULER = "simple"
    OVERLOAD_ALG = "breakwater"
    NUM_CORES_GUARANTEED = 0
    UTILIZATION_RANGE = False
    print(f"{POLICY} has been chosen.")
elif POLICY.lower() == "utilization_range":
    SCHEDULER = "simple"
    OVERLOAD_ALG = "breakwater"
    NUM_CORES_GUARANTEED = 0
    UTILIZATION_RANGE = True
    util_lower = 0.75
    util_upper = 0.95
elif POLICY.lower() == "static":
    SCHEDULER = "simple" # I don't think scheduler matters here
    OVERLOAD_ALG = "breakwater"
    NUM_CORES_GUARANTEED = 16
    UTILIZATION_RANGE = False
    SPIN_SERVER = True
else:
    raise ValueError("Invalid policy chosen. Please choose between [coresync, caladan, shenango/simple, or utilization range]")

os.makedirs(f"outputs/{POLICY.lower()}_3", exist_ok=True)

CALADAN_INTERVAL = 5
# OVERLOAD_ALG = "breakwater"

# The number of client connections
NUM_CONNS = 100

# List of offered load
# OFFERED_LOADS = [500000, 1000000, 1500000, 2000000, 2500000, 3000000, 3500000, 4000000, 4500000, 5000000, 5500000, 6000000, 6500000, 7000000, 7500000, 8000000]
# OFFERED_LOADS = [100000]
OFFERED_LOADS = [100000 * (i + 1) for i in range(10)]

ENABLE_DIRECTPATH = True
# SPIN_SERVER = False # disabling, I think we default to caladan?
DISABLE_WATCHDOG = False

NUM_CORES_SERVER = 16
NUM_CORES_CLIENT = 16

slo = 50
# POPULATING_LOAD = 200000

BREAKWATER_TIMESERIES = True

############################
### End of configuration ###
############################

# Verify configs #
if OVERLOAD_ALG not in ["breakwater", "seda", "dagor", "nocontrol"]:
    print("Unknown overload algorithm: " + OVERLOAD_ALG)
    exit()

cmd = "sed -i'.orig' -e \'s/#define SBW_RTT_US.*/#define SBW_RTT_US\\t\\t\\t{:d}/g\'"\
        " configs/bw_config.h".format(NET_RTT)
execute_local(cmd)

### Function definitions ###
def generate_shenango_config(is_server ,conn, ip, netmask, gateway, num_cores,
        directpath, spin, disable_watchdog):
    config_name = ""
    config_string = ""
    if is_server:
        config_name = "server.config"
        config_string = "host_addr {}".format(ip)\
                      + "\nhost_netmask {}".format(netmask)\
                      + "\nhost_gateway {}".format(gateway)\
                      + "\nruntime_kthreads {:d}".format(num_cores)\
                      + "\nruntime_priority lc"\
                      + "\nruntime_guaranteed_kthreads {:d}".format(NUM_CORES_GUARANTEED)
        if UTILIZATION_RANGE:
            config_string += "\nruntime_util_lower_thresh {:f}".format(util_lower)
            config_string += "\nruntime_util_upper_thresh {:f}".format(util_upper)
        if POLICY.lower() == "coresync" and OVERLOAD_ALG == "breakwater":
            print("breakwater prevent parking going into server config")
            config_string += "\nbreakwater_prevent_parks {:f}".format(SBW_CORE_PARK_TARGET) # I don't think we want this behavior to be on anything but netbench w/breakwater
            config_string += "\nbreakwater_core_credit_ratio {:d}".format(CORE_CREDIT_RATIO)
    else:
        config_name = "client.config"
        config_string = "host_addr {}".format(ip)\
                      + "\nhost_netmask {}".format(netmask)\
                      + "\nhost_gateway {}".format(gateway)\
                      + "\nruntime_kthreads {:d}".format(num_cores)

    if spin:
        config_string += "\nruntime_spinning_kthreads {:d}".format(num_cores)

    if directpath:
        config_string += "\nenable_directpath 1"

    if disable_watchdog:
        config_string += "\ndisable_watchdog 1"
    
    print(f"Config: {config_name}")
    print(config_string)
    print()
    
    if is_server:
        cmd = "cd ~/{}/silo && echo \"{}\" > {} "\
                .format(ARTIFACT_PATH,config_string, config_name)
    else:
        cmd = "cd ~/{}/silo-client && echo \"{}\" > {} "\
                .format(ARTIFACT_PATH,config_string, config_name)
    return execute_remote([conn], cmd, True)
### End of function definition ###

NUM_AGENT = len(AGENTS)

# configure Shenango IPs for config
server_ip = "192.168.1.200"
client_ip = "192.168.1.100"
agent_ips = []
netmask = "255.255.255.0"
gateway = "192.168.1.1"

for i in range(NUM_AGENT):
    agent_ip = "192.168.1." + str(101 + i);
    agent_ips.append(agent_ip)

# k = paramiko.RSAKey.from_private_key_file(KEY_LOCATION)
k = paramiko.Ed25519Key.from_private_key_file(KEY_LOCATION)
# connection to server
server_conn = paramiko.SSHClient()
server_conn.set_missing_host_key_policy(paramiko.AutoAddPolicy())
server_conn.connect(hostname = SERVERS[0], username = USERNAME, pkey = k)

# connection to client
client_conn = paramiko.SSHClient()
client_conn.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client_conn.connect(hostname = CLIENT, username = USERNAME, pkey = k)

# connections to agents
agent_conns = []
for agent in AGENTS:
    agent_conn = paramiko.SSHClient()
    agent_conn.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    agent_conn.connect(hostname = agent, username = USERNAME, pkey = k)
    agent_conns.append(agent_conn)

# Clean-up environment
print("Cleaning up machines...")
cmd = "sudo killall -9 silotpcc-shenango & sudo killall -9 iokerneld"
execute_remote([server_conn], cmd, True, False)

cmd = "sudo killall -9 silo-client & sudo killall -9 iokerneld"
execute_remote([client_conn] + agent_conns,
               cmd, True, False)
sleep(1)

if BREAKWATER_TIMESERIES:
    cmd = "cd ~/{}/{}/breakwater && sed -i \'s/#define SBW_TS_OUT.*/#define SBW_TS_OUT\\t\\t\\t true/\'"\
        " src/bw_server.c".format(ARTIFACT_PATH, KERNEL_NAME)
    execute_remote([server_conn], cmd)
else:
    cmd = "cd ~/{}/{}/breakwater && sed -i \'s/#define SBW_TS_OUT.*/#define SBW_TS_OUT\\t\\t\\t false/\'"\
        " src/bw_server.c".format(ARTIFACT_PATH, KERNEL_NAME)
    execute_remote([server_conn], cmd)


# exit(0)

# Distribuing config files
print("Distributing configs...")
# - server
cmd = "scp -P 22 -i {} -o StrictHostKeyChecking=no configs/*"\
        " {}@{}:~/{}/caladan/breakwater/src/ >/dev/null"\
        .format(KEY_LOCATION, USERNAME, SERVERS[0], ARTIFACT_PATH)
execute_local(cmd)



# - client
cmd = "scp -P 22 -i {} -o StrictHostKeyChecking=no configs/*"\
        " {}@{}:~/{}/caladan/breakwater/src/ >/dev/null"\
        .format(KEY_LOCATION, USERNAME, CLIENT, ARTIFACT_PATH)
execute_local(cmd)
# - agents
for agent in AGENTS:
    cmd = "scp -P 22 -i {} -o StrictHostKeyChecking=no configs/*"\
            " {}@{}:~/{}/caladan/breakwater/src/ >/dev/null"\
            .format(KEY_LOCATION, USERNAME, agent, ARTIFACT_PATH)
    execute_local(cmd)


# # getting new memcached in there
# print("replacing memcached-client")
# # - client
# cmd = "scp -P 22 -i {} -o StrictHostKeyChecking=no memcached-client/mcclient.cc"\
#         " {}@{}:~/{}/memcached-client/ >/dev/null"\
#         .format(KEY_LOCATION, USERNAME, CLIENT, ARTIFACT_PATH)
# execute_local(cmd)
# # - agents
# for agent in AGENTS:
#     cmd = "scp -P 22 -i {} -o StrictHostKeyChecking=no memcached-client/mcclient.cc"\
#             " {}@{}:~/{}/memcached-client/ >/dev/null"\
#             .format(KEY_LOCATION, USERNAME, agent, ARTIFACT_PATH)
#     execute_local(cmd)


# Generating config files
print("Generating config files...")
generate_shenango_config(True, server_conn, server_ip, netmask, gateway,
                         NUM_CORES_SERVER, ENABLE_DIRECTPATH, SPIN_SERVER, DISABLE_WATCHDOG)
generate_shenango_config(False, client_conn, client_ip, netmask, gateway,
                         NUM_CORES_CLIENT, ENABLE_DIRECTPATH, True, False)
for i in range(NUM_AGENT):
    generate_shenango_config(False, agent_conns[i], agent_ips[i], netmask,
                             gateway, NUM_CORES_CLIENT, ENABLE_DIRECTPATH, True, False)

# Rebuild Shanango
print("Building Shenango...")
cmd = "cd ~/{}/caladan && make clean && make && make -C bindings/cc"\
        .format(ARTIFACT_PATH)
execute_remote([server_conn, client_conn] + agent_conns,
               cmd, True)

# Build Breakwater
print("Building Breakwater...")
cmd = "cd ~/{}/caladan/breakwater && make clean && make && make -C bindings/cc"\
        .format(ARTIFACT_PATH)
execute_remote([server_conn, client_conn] + agent_conns,
                 cmd, True)

# Build Silo
print("Building Silo...")
cmd = "cd ~/{}/silo && make clean && ./remake-silo.sh"\
        .format(ARTIFACT_PATH)
execute_remote([server_conn], cmd, True)

# Build McClient
print("Building Silo client...")
cmd = "cd ~/{}/silo-client && make clean && make"\
        .format(ARTIFACT_PATH)
execute_remote([client_conn] + agent_conns, cmd, True)

# # Execute IOKernel
iok_sessions = []
print("Executing IOKernel...")
cmd = "cd ~/{}/caladan && sudo ./iokerneld simple > ../iokernel_debug.out".format(ARTIFACT_PATH)
iok_sessions += execute_remote([client_conn] + agent_conns,
                               cmd, False)


# sleep(2)

# # exit(0)
# # Start silo
# print("Starting Silo server...")
# cmd = "cd ~/{}/silo && sudo LD_LIBRARY_PATH=$(dirname $(find . -name \"liblz4.so\")) ./silotpcc-shenango server.config {} 1 8001 3221225472 > stdout.out"\
#         .format(ARTIFACT_PATH, OVERLOAD_ALG)
# print("Command to run silo server:", cmd)
# server_session = execute_remote([server_conn], cmd, False)
# server_session = server_session[0]

sleep(2)
# print("Populating entries...")
# cmd = "cd ~/{} && sudo ./memcached-client/mcclient {} client.config client {:d} {} SET"\
#         " {:d} {:d} {:d} {:d} 0 >stdout.out 2>&1"\
#         .format(ARTIFACT_PATH, OVERLOAD_ALG, NUM_CONNS, server_ip, MAX_KEY_INDEX,
#                 slo, 0, POPULATING_LOAD)

# client_session = execute_remote([client_conn], cmd, False)
# client_session = client_session[0]

# client_session.recv_exit_status()

sleep(1)

# Remove temporary output
cmd = "cd ~/{}/silo-client && rm output.csv output.json stdout.out".format(ARTIFACT_PATH)
execute_remote([client_conn], cmd, True, False)

sleep(1)

for offered_load in OFFERED_LOADS:
    
    # Execute IOKernel
    iok_sessions = []
    print("Executing IOKernel...")
    if UTILIZATION_RANGE:
        cmd = "cd ~/{}/caladan && sudo ./iokerneld simple range_policy interval {:d} > ../iokernel_debug.out".format(ARTIFACT_PATH, CALADAN_INTERVAL)
    else:
        cmd = "cd ~/{}/caladan && sudo ./iokerneld {} interval {:d} > ../iokernel_debug.out".format(ARTIFACT_PATH, SCHEDULER, CALADAN_INTERVAL)
    iok_sessions += execute_remote([server_conn],
                                cmd, False)


    sleep(4)

    # exit(0)
    # Start silo
    print("Starting Silo server...")
    cmd = "cd ~/{}/silo && sudo LD_LIBRARY_PATH=$(dirname $(find . -name \"liblz4.so\")) ./silotpcc-shenango server.config {} 1 8001 3221225472 > stdout.out"\
            .format(ARTIFACT_PATH, OVERLOAD_ALG)
    print("Command to run silo server:", cmd)
    server_session = execute_remote([server_conn], cmd, False)
    server_session = server_session[0]
    
    sleep(2)
    
    print("Load = {:d}".format(offered_load))
    # - clients
    print("\tExecuting client...")
    client_agent_sessions = []
    cmd = "cd ~/{}/silo-client && sudo ./silo-client {} client.config client {:d} {}"\
            " {:d} {:d} {:d} 0 >> stdout.out 2>&1"\
            .format(ARTIFACT_PATH, OVERLOAD_ALG, NUM_CONNS, server_ip, slo, NUM_AGENT, offered_load)
    print(f"Command to run Silo client for {offered_load} mrps: {cmd}")
    client_agent_sessions += execute_remote([client_conn], cmd, False)

    sleep(1)

    # - Agents
    print("\tExecuting agents...")
    cmd = "cd ~/{}/silo-client && sudo ./silo-client {} client.config agent {}"\
            " >> stdout.out 2>&1".format(ARTIFACT_PATH, OVERLOAD_ALG, client_ip)
    print(f"Command to run Silo agent for {offered_load} mrps: {cmd}")
    client_agent_sessions += execute_remote(agent_conns, cmd, False)
    # Wait for client and agents
    print("\tWaiting for client and agents...")
    for client_agent_session in client_agent_sessions:
        client_agent_session.recv_exit_status()

    sleep(2)
    
    cmd = "sudo killall -9 iokerneld silotpcc-shenango"
    execute_remote([server_conn], cmd, True)
    
    cmd = "scp -P 22 -i {} -o StrictHostKeyChecking=no {}@{}:~/{}/silo/timeseries.csv ./outputs/{}_3/{}_timeseries.csv"\
        " >/dev/null".format(KEY_LOCATION, USERNAME, SERVERS[0], ARTIFACT_PATH, POLICY.lower(), offered_load)
    execute_local(cmd)
    
    # cmd = "sudo killall -9 iokerneld"
    # execute_remote([client_conn] + agent_conns, cmd, True)
    
    sleep(4)


# Kill server
# cmd = "sudo killall -9 silotpcc-shenango"
# execute_remote([server_conn], cmd, True)

# Wait for the server
server_session.recv_exit_status()

# Kill IOKernel
cmd = "sudo killall -9 iokerneld"
execute_remote([client_conn] + agent_conns, cmd, True)

# Wait for IOKernel sessions
for iok_session in iok_sessions:
    iok_session.recv_exit_status()

# Close connections
server_conn.close()
client_conn.close()
for agent_conn in agent_conns:
    agent_conn.close()

# Create output directory
if not os.path.exists("outputs"):
    os.mkdir("outputs")

# Move output.csv and output.json
print("Collecting outputs...")
cmd = "scp -P 22 -i {} -o StrictHostKeyChecking=no {}@{}:~/{}/silo-client/output.csv ./"\
        " >/dev/null".format(KEY_LOCATION, USERNAME, CLIENT, ARTIFACT_PATH)
execute_local(cmd)

output_prefix = "{}".format(POLICY.lower())

if SPIN_SERVER:
    output_prefix += "_spin"

if DISABLE_WATCHDOG:
    output_prefix += "_nowd"

output_prefix += "_silo_nconn_{:d}_nodes_{:d}_{}".format(NUM_CONNS, len(CLIENTS) + 1, OVERLOAD_ALG)

# Print Headers
header = "num_clients,offered_load,throughput,goodput,cpu,min,mean,p50,p90,p99,p999,p9999"\
        ",max,lmin,lmean,lp50,lp90,lp99,lp999,lp9999,lmax,p1_win,mean_win,p99_win,p1_q,mean_q,p99_q,server:rx_pps"\
        ",server:tx_pps,server:rx_bps,server:tx_bps,server:rx_drops_pps,server:rx_ooo_pps"\
        ",server:winu_rx_pps,server:winu_tx_pps,server:win_tx_wps,server:req_rx_pps"\
        ",server:resp_tx_pps,client:min_tput,client:max_tput"\
        ",client:winu_rx_pps,client:winu_tx_pps,client:resp_rx_pps,client:req_tx_pps"\
        ",client:win_expired_wps,client:req_dropped_rps"
cmd = "echo \"{}\" > outputs/{}_3/{}.csv".format(header, POLICY.lower(), output_prefix)
execute_local(cmd)

cmd = "cat output.csv >> outputs/{}_3/{}.csv".format(POLICY.lower(), output_prefix)
execute_local(cmd)

# Remove temp outputs
cmd = "rm output.csv"
execute_local(cmd, False)

print("Output generated: outputs/{}_3/{}.csv".format(POLICY.lower(), output_prefix))
print("Done.")

