#!/usr/bin/env python

import struct
import time

import inspect, os, sys
# From http://stackoverflow.com/questions/279237/python-import-a-module-from-a-folder
cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if cmd_subfolder not in sys.path:
    sys.path.insert(0, cmd_subfolder)

import mosq_test
import vmq

rc = 1
keepalive = 60
connect_packet = mosq_test.gen_connect("will-null-topic", keepalive=keepalive, will_topic="", will_payload=struct.pack("!4sB7s", "will", 0, "message"))
connack_packet = mosq_test.gen_connack(rc=2)

vmq.start('default.conf')

try:
    time.sleep(0.5)

    sock = mosq_test.do_client_connect(connect_packet, "", timeout=30)
    rc = 0
    sock.close()
finally:
    vmq.stop()

exit(rc)
