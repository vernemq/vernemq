#!/usr/bin/env python

import time

import inspect, os, sys
# From http://stackoverflow.com/questions/279237/python-import-a-module-from-a-folder
cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if cmd_subfolder not in sys.path:
    sys.path.insert(0, cmd_subfolder)

import mosq_test
import vmq

rc = 1
mid = 53
keepalive = 60
connect_packet_ok = mosq_test.gen_connect("will-acl-test", keepalive=keepalive, will_topic="ok", will_payload="should be ok")
connack_packet_ok = mosq_test.gen_connack(rc=0)

connect_packet = mosq_test.gen_connect("will-acl-test", keepalive=keepalive, will_topic="will/acl/test", will_payload="should be denied")
connack_packet = mosq_test.gen_connack(rc=5)

vmq.start('07-will-acl-denied.conf')

try:
    time.sleep(0.5)

    sock_ok = mosq_test.do_client_connect(connect_packet_ok, connack_packet_ok, timeout=5, connack_error="connack ok")
    sock = mosq_test.do_client_connect(connect_packet, connack_packet)
    rc = 0
    sock.close()
    sock_ok.close()
finally:
    vmq.stop()

exit(rc)
