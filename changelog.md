# Changelog

## VERNEMQ 0.13.1

### vmq_server

- Major refactoring of the metrics subsystem:
    Graphite and Systree metric reporters are now part of vmq_server and don't
    rely on exometer_core anymore. Instead of exometer_core a more efficient Erlang
    NIF based counter implementation is used (mzmetrics). As a result of superseding
    exometer_core many new metrics are available now, and most of the old ones don't
    exist anymore. With the removal of the exometer_core dependency we gave up the
    native support for SNMP (in vmq_snmp). We introduce a Prometheus metrics HTTP 
    handler, which enables the great open source monitoring solution Prometheus 
    (with a rich set of reporters, including SNMP) to scrape metrics from VerneMQ. 
    Please check the [documentation](https://vernemq.com/docs/monitoring/) and 
    adjust your monitoring solutions.

- LevelDB message store performance improvement for QoS 1 and 2 message

- Fix a memory leak in message retry mechanism

- Fix a message leak in the LevelDB message store:
    Since VerneMQ 0.12.5p4 the leaked messages were detected on broker start and
    automatically garbage collected.

- Simplify changing the nodename in case of a single node cluster

- MQTT conformance, session present MQTT-3.2.2-2

- MQTT conformance, will message must not be published if client sends
    'DISCONNECT' message: MQTT-3.1.2-8

- MQTT conformance, unauthorized publishes are positively acknowledged: MQTT-3.3.5-2

- MQTT conformance, pass empty retained message on to subscribers: MQTT-3.3.1-11

- Several minor bugs fixes

### vmq_acl

- Lines starting with '#' are treated as comments

- Added mountpoint pattern '%m' support for pattern matches

- Minor bug fixes

### vmq_passwd

- Minor bug fixes

### vmq_systree

- Not included anymore, systree metrics are now reported by `vmq_server`:
    Make sure to check the new configuration for systree. Please check the 
    documentation and adjust your monitoring solutions.

### vmq_graphite

- Not included anymore, graphite metrics are now reported by `vmq_server`:
    Make sure to check the new configuration for graphite. Please check the 
    documentation and adjust your monitoring solutions.

### vmq_snmp

- Not included anymore, SNMP isn't natively supported anymore:
    `vmq_server` has a new HTTP metrics handler that exports all metrics in the
    Prometheus format. Prometheus is an open source monitoring solution that has
    multiple options for reporting metrics, including SNMP.

### vmq_commons

- Minor bug fixes

### General

- Binary packages are now using Erlang OTP 18.3, this requires a recompilation of
    your custom Erlang plugins. 
    

## VERNEMQ 0.12.5

### vmq_server

- Major refactoring that enables a more scalable and robust migration of online
    and offline queues.

- Use of new Rebar3 including the use of hex packages whenever possible.

- Support for Erlang 18.

- Reworked interal publish interface for plugins that require more control over
    the way VerneMQ publishes their messages (e.g. setting the dup & retain flags).
    Plugins which used the old 'PublishFun/2' MUST use the new 'PublishFun/3' which
    is specified as follows: 
        
        `fun([Word:binary()|_] = Topic, Payload:binary(), Opts:map()) -> ok | {error, Reason}`

- Several minor bug fixes.

- Several third-party dependencies have been upgraded.

### General

- Use of new Rebar3 and use of hex packages in all our own dependencies.

- The docs are now hosted on our website http://verne.mq/docs.


## VERNEMQ 0.12.0

This release increases overall stability and performance. Memory usage for the subscriber store
has been reduced by 40-50%. To achieve this, we had to change the main topic structure, and unfortunately this 
breaks backward compatibility for the stored offline data as well as the plugin system.

.. warning::

    Make sure to delete the old message store folder ``/var/lib/vernemq/msgstore``
    and the old metadata folder ``/var/lib/vernemq/meta``.

    If you have implemented your own plugins, make sure to adapt to the new topic
    format. (Reach out to us if you require assistance).

### vmq_server

- Major refactoring of the MQTT state machine:
    We reduced the number of processes per connected client and MQTT state machine.
    This leads to less gen_server calls, fewer inter-process messages and a reduced scheduler load.
    Per client backpressure can be applied in a more direct way.

- New topic format:
    Topics are now essentially list of binaries e.g. the topic ``hello/+/world``
    gets parsed to a ``[<<"hello">>, <<"+">>, <<"world">>]``.
    Therefore every API that used a topic as an argument had to be changed.

- Improved cluster leave, and queue migration:
    This allows an operator to make a node gracefully leave the cluster. 
    1. If the node is still online and part of the cluster a two step approach is used. 
    During the first step, the node stops accepting new connections, but keeps serving
    the existing ones. In the second step it actively kills the online sessions,
    and (if possible) migrates their queue contents to the other cluster nodes. Once
    all the queues are migrated, the node leaves the cluster and terminates itself.
    2. If the node is already offline/crashed during a cluster leave call, the old subscriptions 
    on that node are remapped to the other cluster nodes. VerneMQ gets the information to do
    this mapping from the Plumtree Metadata store. No offline messages can be copied in this case.

- New hook, ``on_deliver/4``:
    Every message that gets delivered passes this hook, which allows a plugin to
    log the message and much more if needed (change payload and topic at 
    delivery etc)

- New hook, ``on_offline_message/1``:
    If a client has been connected with ``clean_session=false`` every message that
    gets offline-queued triggers this hook. This would be the entrypoint to
    call mobile push notification services.

- New hook, ``on_client_wakeup/1``:
    When an authenticated client attaches to its queue this hook is triggered.

- New hook, ``on_client_offline/1``:
    When a client with ``clean_session=false`` disconnects this hook is triggered.

- New hook, ``on_client_gone/1``:
    When a client with ``clean_session=true`` disconnects this hook is triggered.
    
- No RPC calls anymore during registration and queue migration flows.

- Many small bug fixes and improvements.

### vmq_commons

- New topic parser and validator

- New shared behaviours for the new hooks

### vmq_acl

- Adapt to use the new topic format

### vmq_bridge

- Adapt to use the new topic format

### vmq_systree

- Adapt to use the new topic format


## VERNEMQ 0.11.0

The queuing mechanism got a major refactoring. Prior to this version the offline
messages were stored in a global in-memory table backed by the leveldb based
message store. Although performance was quite ok using this approach, we were
lacking of flexibility regarding queue control. E.g. it wasn't possible to limit
the maximum number of offline messages per client in a straightforward way. This
and much more is now possible. Unfortunately this breaks backward compatibility
for the stored offline data.

.. warning::

    Make sure to delete (or backup) the old message store folder 
    ``/var/lib/vernemq/msgstore`` and the old metadata folder ``/var/lib/vernemq/meta``.

    We also updated the format of the exposed client metrics. Make sure
    to adjust your monitoring setup.

### vmq_server

- Major refactoring for Queuing Mechanism:
    Prior to this version the offline messages were stored in a global ETS bag
    table, which was backed by the LevelDB powered message store. The ETS table
    served as an index to have a fast lookup for offline messages. Moreover this
    table was also used to keep the message references. All of this changed. 
    Before every client session was load protected by an Erlang process that
    acted as a queue. However, once the client has disconnected this process had
    to terminate. Now, this queue process will stay alive as long as the session
    hasn't expired and stores all the references to the offline messages. This 
    simplifies a lot. Namely the routing doesn't have to distinguish between
    online and offline clients anymore, limits can be applied on a per client/queue
    basis, gained more flexibility to deal with multiple sessions.

- Major refactoring for Message Store
    The current message store relies only on LevelDB and no intermediate
    ETS tables are used for caching/indexing anymore. This improves overall memory
    overhead and scalability. However this breaks backward compatibility and
    requires to delete the message store folder.

- Changed Supervisor Structure for Queue Processes
    The supervisor structure for the queue processes changed in a way that 
    enables much better performance regarding setup and teardown of the queue 
    processes.

- Improved Message Reference Generation Performance

- Upgraded to newest verison of Plumtree

- Upgraded to Lager 3.0.1 (required to pretty print maps in log messages)

- Many smaller fixes and cleanups


### vmq_commons

- Better error messages in case of parsing errors

- Fixed a parser bug with very small TCP segments



### VERNEMQ 0.10.0

We switched to the rebar3 toolchain for building VerneMQ, involving quite some 
changes in many of our direct dependencies (``vmq_*``). Several bug fixes and
performance improvements. Unfortunately some of the changes required some backward
imcompatibilites:

.. warning::

    Make sure to delete (or backup) the old subscriber data directory 
    ``/var/lib/vernemq/meta/lvldb_cluster_meta`` as the contained data format isn't
    compatible with the one found in ``0.10.0``. Durable sessions (``clean_session=false``) 
    will be lost, and the clients are forced to resubscribe.
    Although the offline messages for these sessions aren't necessary lost, an ugly
    workaround is required. Therefore it's recommended to also delete the message store
    folder ``/var/lib/vernemq/msgstore``.

    If you were running a clustered setup, make sure to revisit the clustering
    documentation as the specific ``listener.vmq.clustering`` configuration is needed 
    inside ``vernemq.conf`` to enable inter-node communicaton.
    
    We updated the format of the exposed metrics. Make sure to adjust your monitoring setup.

### vmq_server

- Changed application statistics:
    Use of more efficient counters. This changes the format of the values
    obtained through the various monitoring plugins.
    Added new system metrics for monitoring overall system health.
    Check the updated docs.

- Bypass Erlang distribution for all distributed MQTT Publish messages:
    use of distinct TCP connections to distribute MQTT messages to other cluster
    nodes. This requires to configure a specific IP / port in the vernemq.conf.
    Check the updated docs. 

- Use of more efficient key/val layout within the subscriber store:
    This allows us to achieve higher routing performance as well as keeping
    less data in memory. On the other hand the old subscriber store data (not
    the message store) isn't compatible with the new one. Since VerneMQ is still
    pretty young we won't provide a migration script. Let us know if this is a
    problem for you and we might find a solution for this. Removing the
    '/var/lib/vernemq/meta/lvldb_cluster_meta' folder is necessary to successfully
    start a VerneMQ node.

- Improve the fast path for QoS 0 messages:
    Use of non-blocking enqueue operation for QoS 0 messages. 

- Performance improvements by reducing the use of timers throughout the stack:
    Counters are now incrementally published, this allowed us to remove a
    timer/connection that was triggered every second. This might lead to
    accuracy errors if sessions process a very low volume of messages.
    Timers for process hibernation are removed since process hibernation isn't really 
    needed at this point. Moreover we got rid of the CPU based automatic throttling 
    mechanism which used timers to delay the accepting of new TCP packets.

- Improved CLI handling:
    improved 'cluster leave' command and better help text.

- Fixed several bugs found via dialyzer


### vmq_commons

- Multiple conformance bug fixes:
    Topic and subscription validation

- Improved generic gen_emqtt client

- Added correct bridge protocol version

- Fixed bugs found via dialyzer

### vmq_snmp 

- Merged updated SNMP reporter from feuerlabs/exometer

- Cleanup of unused OTP mibs (the OTP metrics are now directly exposed by vmq_server)


### vmq_plugin

- Minor bug fixed related to dynamically loading plugins

- Switch to rebar3 (this includes plugins following the rebar3 structure)
