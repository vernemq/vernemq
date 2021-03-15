#!/usr/bin/env bash
set -e

if [ "$1" = "swc" ]
then
    make swc
    mv _build/swc/rel/vernemq release
else
    make rel
    mv _build/default/rel/vernemq release
fi
