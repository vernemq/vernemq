#!/usr/bin/env python

# Test whether a SUBSCRIBE to a topic with QoS 2 results in the correct SUBACK packet.

import time

import inspect, os, sys
# From http://stackoverflow.com/questions/279237/python-import-a-module-from-a-folder
cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if cmd_subfolder not in sys.path:
    sys.path.insert(0, cmd_subfolder)

import mosq_test
import vmq

rc = 1
mid = 3
keepalive = 60
connect_packet = mosq_test.gen_connect("subscribe-qos2-test", keepalive=keepalive)
connack_packet = mosq_test.gen_connack(rc=0)

subscribe_packet = mosq_test.gen_subscribe(mid, "qos2/test", 2)
suback_packet = mosq_test.gen_suback(mid, 2)

vmq.start('default.conf')

try:
    time.sleep(0.5)

    sock = mosq_test.do_client_connect(connect_packet, connack_packet)
    sock.send(subscribe_packet)

    if mosq_test.expect_packet(sock, "suback", suback_packet):
        rc = 0

    sock.close()
finally:
    vmq.stop()

exit(rc)

