#!/bin/sh

epmd -daemon
apt-get install -y pcregrep
mkdir -p /vernemq/_build/all_tests+test/logs
sh ./run-tests-with-retry.sh /vernemq