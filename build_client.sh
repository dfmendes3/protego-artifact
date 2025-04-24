#!/bin/bash

set -e
set -x

# record BASE_DIR
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
echo "BASE_DIR = '${SCRIPTPATH}/'" > base_dir.py

git submodule update --init -f --recursive caladan

# apply breakwater patches
# NOTE this will break current CoreSync, patches were applied as a commit to reconcile with efficient scheduling commits
# . init_submodules.sh

if lspci | grep -q 'ConnectX-5'; then
 sed "s/CONFIG_MLX5=.*/CONFIG_MLX5=y/g" -i caladan/build/config
 sed "s/CONFIG_DIRECTPATH=.*/CONFIG_DIRECTPATH=y/g" -i caladan/build/config
elif lspci | grep -q 'ConnectX-4'; then
 sed "s/CONFIG_MLX5=.*/CONFIG_MLX5=y/g" -i caladan/build/config
elif lspci | grep -q 'ConnectX-3'; then
 sed "s/CONFIG_MLX4=.*/CONFIG_MLX4=y/g" -i caladan/build/config
fi
# probably doesn't hurt, but I don't think I need spdk running, just dpdk
# sed "s/CONFIG_SPDK=.*/CONFIG_SPDK=y/g" -i caladan/build/config
# TODO remove, just debugging for now
# sed "s/CONFIG_DEBUG=.*/CONFIG_DEBUG=y/g" -i caladan/build/config
# sed "s/O0/O3/" -i caladan/build/shared.mk

# TODO These seem to be not working, and it's forcing sequential with the jobserver
pushd caladan
make submodules -j16
make -j16

pushd ksched
sudo rm -rf build
make -j16
popd

# echo building LOADGEN
# pushd apps/synthetic
# cargo build --release
# popd

popd
