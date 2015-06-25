VERNEMQ 0.10.1
==============

vmq_server
----------

- changed application statistics:
    use of more efficient counters. this changes the format of the values
    obtained through the various monitoring plugins.
    added new system metrics for monitoring overall system heatlth.
    check the updated docs.

- bypass erlang distribution for all messages leaving a node:
    use of distinct TCP connections to distribute MQTT messages to other cluster
    nodes. this requires to configure a specific IP / port in the vernemq.conf.
    check the updated docs. 

- use of more efficient key/val layout within the subscriber store:
    this allows us to achieve higher routing performance as well as keeping
    less data in memory. On the other hand the old subscriber store data (not
    the message store) isn't compatible with the new one. Since VerneMQ is still
    pretty young we won't provide a migration script. Let us know if this is a
    problem for you and we might find a solution for this. Removing the
    '/var/lib/vernemq/meta/lvldb_cluster_meta' folder is necessary to successfuly
    start a vernemq node.

- improve the fast path for QoS 0 messages:
    use of non-blocking enqueue operation for QoS 0 messages. 

- performance improvements by reducing the use of timers throughout the stack:
    counters are now updated incrementally, this allowed us to remove a
    timer/connection that was triggered every second. This might lead to
    accuracy errors if sessions process a very low volume of messages.
    remove timers for process hibernation since hibernation isn't really 
    needed at this point. Moreover we removed the CPU based automatic throttling 
    mechanism which used timers to delay the accepting of new TCP packets.

- improved CLI handling:
    improved 'cluster leave' command and better help text.

- switch to the rebar3 toolchain:
    this introduced changes in the whole release and packaging toolchain.

- fixed bugs found via dialyzer

- switch to rebar3


vmq_commons
-----------

- multiple conformance bug fixes:
    topic and subscription validation

- improved generic gen_emqtt

- added correct bridge protocol version

- fixed bugs found via dialyzer

- switch to rebar3


vmq_systree
-----------

- switch to rebar3


vmq_snmp 
--------

- merged new snmp reporter from feuerlabs/exometer

- cleanup of unused otp mibs (the otp metrics are now exposed by vmq_server)

- switch to rebar3


vmq_plugin
----------

- switch to rebar3 (this includes plugins following the rebar3 structure)


vmq_graphite
------------

- switch to rebar3


vmq_acl
-------

- switch to rebar3


vmq_passwd
----------

- switch to rebar3


vmq_bridge
----------

- switch to rebar3
