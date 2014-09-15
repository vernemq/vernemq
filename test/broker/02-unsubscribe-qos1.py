#!/usr/bin/env python

# Test whether a SUBSCRIBE to a topic with QoS 1 results in the correct SUBACK packet.

import time

import inspect, os, sys
# From http://stackoverflow.com/questions/279237/python-import-a-module-from-a-folder
cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if cmd_subfolder not in sys.path:
    sys.path.insert(0, cmd_subfolder)

import mosq_test
import emqttd

rc = 1
mid = 79
keepalive = 60
connect_packet = mosq_test.gen_connect("unsubscribe-qos1-test", keepalive=keepalive)
connack_packet = mosq_test.gen_connack(rc=0)

unsubscribe_packet = mosq_test.gen_unsubscribe(mid, "qos1/test")
unsuback_packet = mosq_test.gen_unsuback(mid)

emqttd.start('default.conf')

try:
    time.sleep(0.5)

    sock = mosq_test.do_client_connect(connect_packet, connack_packet)
    sock.send(unsubscribe_packet)

    if mosq_test.expect_packet(sock, "unsuback", unsuback_packet):
        rc = 0

    sock.close()
finally:
    emqttd.stop()

exit(rc)

