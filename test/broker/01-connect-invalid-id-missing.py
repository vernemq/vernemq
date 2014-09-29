#!/usr/bin/env python

# Test whether a CONNECT with a zero length client id results in the correct CONNACK packet.

import time

import inspect, os, sys
# From http://stackoverflow.com/questions/279237/python-import-a-module-from-a-folder
cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if cmd_subfolder not in sys.path:
    sys.path.insert(0, cmd_subfolder)

import mosq_test
import vmq

rc = 1
keepalive = 10
connect_packet = mosq_test.gen_connect(None, keepalive=keepalive)

vmq.start('default.conf')

try:
    time.sleep(0.5)

    sock = mosq_test.do_client_connect(connect_packet, "")
    sock.close()
    rc = 0
finally:
    vmq.stop()

exit(rc)
