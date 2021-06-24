#!/bin/sh

epmd -daemon
./rebar3 as all_tests do dialyzer, eunit, ct
