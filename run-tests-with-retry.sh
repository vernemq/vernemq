#!/bin/sh

DIR=${1:-.}
FULLPATH=$DIR/_build/all_tests+test/logs/retry.spec

echo $FULLPATH

./rebar3 as all_tests dialyzer || echo "Dialyzer failed!!!"

./rebar3 as all_tests do eunit, ct ||
      (echo -e "\nContents of retry.spec\n" &&
      cat $FULLPATH &&
      echo -e "\nRetry suites:" &&
      echo $(pcregrep -o2 -o3 --om-separator="/" -M "^{(cases|groups),\"(.+)\",[^\w]*(\w+),(.|\n)*?\.$" $FULLPATH | uniq | paste -s -d, -) &&
      make db-reset &&
      ./rebar3 ct --suite=$(pcregrep -o2 -o3 --om-separator="/" -M "^{(cases|groups),\"(.+)\",[^\w]*(\w+),(.|\n)*?\.$" $FULLPATH | uniq | paste -s -d, -))
