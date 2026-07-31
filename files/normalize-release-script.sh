#!/bin/sh

set -eu

for release_dir in _build/default/rel/vernemq/releases/*; do
    if [ ! -d "$release_dir" ]; then
        continue
    fi

    if [ -f "$release_dir/vernemq.script" ]; then
        continue
    fi

    if [ -f "$release_dir/start.script" ]; then
        cp "$release_dir/start.script" "$release_dir/vernemq.script"
    fi
done
