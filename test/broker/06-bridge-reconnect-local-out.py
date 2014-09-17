#!/usr/bin/env python

# Test whether a bridge topics work correctly after reconnection.
# Important point here is that persistence is enabled.

import os
import subprocess
import time

import inspect, sys
# From http://stackoverflow.com/questions/279237/python-import-a-module-from-a-folder
cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if cmd_subfolder not in sys.path:
    sys.path.insert(0, cmd_subfolder)

import mosq_test
import emqttd

rc = 1
keepalive = 60
connect_packet = mosq_test.gen_connect("bridge-reconnect-test", keepalive=keepalive)
connack_packet = mosq_test.gen_connack(rc=0)

mid = 180
subscribe_packet = mosq_test.gen_subscribe(mid, "bridge/+", 0)
suback_packet = mosq_test.gen_suback(mid, 0)
publish_packet = mosq_test.gen_publish("bridge/reconnect", qos=0, payload="bridge-reconnect-message")


emqttd.start('default.conf')
time.sleep(0.5)
emqttd.start('06-bridge-reconnect-local-out.conf', broker_path="../emqttd2")
time.sleep(0.5)
emqttd.hard_stop("../emqttd2")
emqttd.start('06-bridge-reconnect-local-out.conf', broker_path="../emqttd2")
time.sleep(5)

pub = None
try:
    time.sleep(0.5)

    sock = mosq_test.do_client_connect(connect_packet, connack_packet)
    sock.send(subscribe_packet)

    if mosq_test.expect_packet(sock, "suback", suback_packet):
        sock.send(subscribe_packet)

        if mosq_test.expect_packet(sock, "suback", suback_packet):
            pub = subprocess.Popen(['./06-bridge-reconnect-local-out-helper.py'], stdout=subprocess.PIPE)
            pub.wait()
            # Should have now received a publish command

            if mosq_test.expect_packet(sock, "publish", publish_packet):
                rc = 0
    sock.close()
finally:
    emqttd.hard_stop("../emqttd2")
    emqttd.hard_stop()

exit(rc)

