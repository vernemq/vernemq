#!/bin/sh

epmd -daemon
apt-get install -y pcregrep
mkdir -p /vernemq/_build/all_tests+test/logs
./rebar3 as all_tests do dialyzer, eunit, ct ||
      (echo -e "\nContents of retry.spec\n" &&
      cat /vernemq/_build/all_tests+test/logs/retry.spec &&
      echo -e "\nRetry suites:" &&
      echo $(pcregrep -o2 -o3 --om-separator="/" -M "^{(cases),\"(.+)\",[^\w]*(\w+),(.|\n)*?\.$" /vernemq/_build/all_tests+test/logs/retry.spec | uniq | paste -s -d, -) &&
      ./rebar3 ct --suite=$(pcregrep -o2 -o3 --om-separator="/" -M "^{(cases),\"(.+)\",[^\w]*(\w+),(.|\n)*?\.$" /vernemq/_build/all_tests+test/logs/retry.spec | uniq | paste -s -d, -))

