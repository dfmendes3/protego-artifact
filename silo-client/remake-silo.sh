#!/bin/bash

set -e

export SHENANGODIR=$(readlink -f $PWD/..)/caladan
echo building SILO-CLIENT in $SHENANGODIR
make