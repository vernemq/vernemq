#!/bin/sh
# -*- tab-width:4;indent-tabs-mode:nil -*-
# ex: ts=4 sw=4 et

# Pull environment for this install
. "{{runner_base_dir}}/lib/env.sh"

# Make sure the user running this script is the owner and/or su to that user
check_user "$@"
ES=$?
if [ "$ES" -ne 0 ]; then
    exit $ES
fi

# Keep track of where script was invoked
ORIGINAL_DIR=$(pwd)

# Make sure CWD is set to runner run dir
cd $RUNNER_BASE_DIR

# Identify the script name
SCRIPT=`basename $0`

case $1 in
    "trace")
        $NODETOOL rpc_infinity vmq_server_cli command $SCRIPT $@;;
    *)
        $NODETOOL rpc vmq_server_cli command $SCRIPT $@;;
esac
