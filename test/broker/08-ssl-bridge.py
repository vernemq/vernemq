#!/usr/bin/env python

import os
import subprocess
import socket
import ssl
import time

import inspect, sys
# From http://stackoverflow.com/questions/279237/python-import-a-module-from-a-folder
cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if cmd_subfolder not in sys.path:
    sys.path.insert(0, cmd_subfolder)

import mosq_test
import vmq

rc = 1
keepalive = 60
client_id = socket.gethostname()+".bridge_test"
connect_packet = mosq_test.gen_connect(client_id, keepalive=keepalive, clean_session=False, proto_ver=128+3)
connack_packet = mosq_test.gen_connack(rc=0)

mid = 1
subscribe_packet = mosq_test.gen_subscribe(mid, "bridge/#", 0)
suback_packet = mosq_test.gen_suback(mid, 0)

publish_packet = mosq_test.gen_publish("bridge/ssl/test", qos=0, payload="message")

vmq.start('08-ssl-bridge.conf')

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
ssock = ssl.wrap_socket(sock, ca_certs="../ssl/all-ca.crt", keyfile="../ssl/server.key", certfile="../ssl/server.crt", server_side=True, ssl_version=ssl.PROTOCOL_TLSv1)
ssock.settimeout(20)
ssock.bind(('', 1888))
ssock.listen(5)


try:
    time.sleep(0.5)

    (bridge, address) = ssock.accept()
    bridge.settimeout(20)

    if mosq_test.expect_packet(bridge, "connect", connect_packet):
        bridge.send(connack_packet)

        if mosq_test.expect_packet(bridge, "subscribe", subscribe_packet):
            bridge.send(suback_packet)

            pub = subprocess.Popen(['./08-ssl-bridge-helper.py'], stdout=subprocess.PIPE)
            pub.wait()

            if mosq_test.expect_packet(bridge, "publish", publish_packet):
                rc = 0

    bridge.close()
finally:
    try:
        bridge.close()
    except NameError:
        pass

    vmq.hard_stop()
    ssock.close()

exit(rc)
