# Changelog

## Not yet released

- Fix typo in configuration name `plumtree.outstandind_limit` should be
  `plumtree.outstanding_limit`.

## VerneMQ 1.2.3

- The shared subscriber feature is no longer marked as in beta.
- Strengthen check verifying if a remote node is available or not. The new check
  verifies that a data connection to the remote node has been established, in
  addition to the current check which verifies that a specific process is running
  on the remote node. This new check will make it more visible if, for instance,
  the IP an port configured via `listener.vmq.clustering` is not reachable or a
  listener was not able to be started.
- Fix issue preventing the proxy_protocol setting (`listener.tcp.proxy_protocol
  = on`) being inherited by specific listeners (#516).
- Remove shared subscriptions backwards compatibility code. Introducing shared
  subscriptions changed some internals and code ensuring backwards compatibility
  had to be written. This code has existed since VerneMQ 1.0.0rc1 and anyone
  needing to upgrade a running cluster from older versions than 1.0.0rc1 of
  VerneMQ will have to first do an intermediate upgrade to a version containing
  the compatibility code.
- Remove time and randomness related dependencies for backwards compatibility
  for OTP 17. These are no longer required as OTP 17 support was removed before
  VerneMQ 1.0.
- Minor optimizations.
- Fix issue in the queue initialization introduced in VerneMQ 1.2.2 which meant
  offline messages were not being read into the queue process after a node
  restart. Added tests to prevent this issue from occurring again.
- Fix crash in cleanup of the `vmq_webhooks` plugin when the plugin is being
  stopped or the broker is shutting down (#556).
- Fix incorrect format strings in `vmq_reg_sup` log statements.
- Add missing argument to HTTP module configuration log statement.
- Do not resolve host names when including peer host in an `vmq-admin session
  show` query as this can lead to a timeout.
- Remove superfluous warnings for ip and port when using the proxy protocol.

## VERNEMQ 1.2.2

- Fixed a number issues when filtering `vmq-admin session show` results. Note
  `msg_ref` filtering is currently broken.
- Fix node startup routine to cleanup stale subscriber data.
- Do not log getting a client peername failed because the client already
  disconnected.

## VERNEMQ 1.2.1

- Upgrade MongoDB driver.
- Prevent unbounded memory growth in a netsplit situation by putting an explicit
  limit (250K) on the number of outstanding plumtree-msgs in plumtree. If this
  limit is exceeded, delayed replication will be handled by active anti-entropy.
- Handle uncaught error type in the `vmq_ql_query` handler.
- Make sure the `peer_host` can always be retrieved via the HTTP API. It was
  returned as an erlang tuple which caused the conversion to JSON to fail.
- Fix issue causing too few results to be returned from `vmq-admin session show`
  when used with filter options. This could happen when terms included in the
  filters also existed as erlang atoms.
- Plugin workflow improvements: move plugin development specific things into
  `vernemq_dev`.
- Fix error in the HTTP API interface. The alias `/api/v1/sessions` mapped to
  `session list` which is deprecated for `session show`.
- Fix `max-age` parse issue in `vmq_webhooks` (#527).

## VERNEMQ 1.2.0

- Fix retain msg server race condition preventing some messages from being
  persisted and replicated to other nodes in the cluster (#507).
- Log when a client is disconnected and multiple sessions are not allowed.
- Fix tracer `mountpoint` parameter bug.
- Make it possible to add/inject HTTP API keys.
- Add Erlang VM memory usage stats to the exposed metrics.
- Fix bug with `max_message_size` and `message_size_limit` only one of these
  should exist and `message_size_limit` has now been deprecated and no longer
  has any effect. `max_message_size` should be used instead.
- Fix bug in `vmq_diversity` where the MongoDB client pool would not reconnect
  after a MongDB restart (#475).
- Fix bug in `vmq_diversity` where the PostgreSQL client pool would not reconnect
  after a PostgreSQL restart (same as #475).

## VERNEMQ 1.1.1

- Fix bug preventing persistent session expiration
  (`persistent_client_expiration` in `vernemq.conf`) from being executed.
- Make `vmq-admin session show` more robust when sessions are overloaded by
  limiting the time allowed to query each session. The default query timeout is
  100ms, but can be overriden using `--rowtimeout=<TimeoutInMilliseconds>`.
- Add support for Erlang/OTP20.
- Improve tracer usage text.
- Fix `vmq_diversity` memcached issue (#460).
- Fix RPM package issue preventing a clean upgrade.
- Fix code path bug related to upgrading plugins.
- Update `plumtree` with a fix to make the plumtree mailbox traversal
  measurement more accurate and therefore less spammy.

## VERNEMQ 1.1.0

- Add more detail to the client expiration log message.
- Safeguard Lua function calls
- Fix bug where websocket connections were incorrectly terminated (#387).
- Fix breakage of event hook chain in `vmq_passwd` (#396).
- Lua script balancing for improved Lua hook performance.
- Add descriptions to all available metrics via `vmq-admin metrics show
  --with-descriptions`.
- Add Prometheus HELP lines for every metric.
- Add a log message (info level) indicating that connectivity to a remote node
  has been restored.
- Minor changes to the formatting of the tracer output.

## VERNEMQ 1.0.1

- Purge node-local clean session true subscriber data when restarting
  VerneMQ as these would otherwise erroneously still exist.
- The metrics reporting sizes of retained messages `gauge.retain_memory` and
  routes `gauge.router_memory` were incorrectly done in system word sizes
  instead of in bytes. This has been corrected. Further the
  `gauge.retain_memory` metric now also includes temporary storage used before
  persisting messages to disk.
- Fix bug causing hooks to be registered multiple times when reloading lua
  scripts (#348).
- Fix bug occurring when publishing across nodes where more than one subscriber
  are on one node and the publisher on another. In this case only one of the
  subscribers would receive the message.
- Fix formatting bug in the `vmq-admin trace` command.
- Handle empty modifier list correctly in `vmq_webhooks` (#339).
- Handle client_id and mountpoint modifiers correctly in `vmq_webhooks` (#332).
- Fix vmq-admin session show when multiple filters are applied
- Fix bug where an aborted connection handshake caused queued messages not 
  being migrated properly to remote node.

## VERNEMQ 1.0.0

- To make the `vmq-admin` tool more consistent, the following changes have been
  made:
  
  - vmq-admin script status -> vmq-admin script show
  - vmq-admin session list -> vmq-admin session show
  - vmq-admin cluster status -> vmq-admin cluster show
  - vmq-admin webhooks status -> vmq-admin webhooks show
  - vmq-admin webhooks cache stats -> vmq-admin webhooks cache show
  - vmq-admin api create-key -> vmq-admin api-key create
  - vmq-admin api delete-key -> vmq-admin api-key delete
  - vmq-admin api list-keys -> vmq-admin api-key show
- Fix a bug in `vmq-admin session list` triggered when running in a cluster.
- Add automatic CRL refreshing, configurable via the hidden
  `crl_refresh_interval` config option. Default is 60 seconds.
- Stop `all_till_ok` hook evaluation if a hook returns anything else than
	`next`.
- Add `vmq-admin trace` tracing sub-system.
- Improve help texts for `vmq-admin listener` command.

### vmq_diversity

- Fix mongodb authentication problem due to a missing dependency.

## VERNEMQ 1.0.0rc2

- Fix wrong lua paths in generated packages.
- Fix connack counter metric
- Fix keepalive timer to fire earliest at 1.5 times the keepalive value
- Improve error reporting for wrong configuration parameters in vernemq.conf
- Fix various Dialyzer type errors

## VERNEMQ 1.0.0rc1

- Add out-of-the-box authentication and authorization support for Postgres, 
  MySQL, MongoDB, and Redis via `vmq_diversity`. 
- Erlang 17.x is no longer officially supported.
- New `rebar3` version (3.3.5), required to upgrade `node_package` dependency.
- The plugins `vmq_webhooks` and `vmq_diversity` are now shipped as part of
  VerneMQ itself.
- Packages are now built using Erlang/OTP 19.2.

### vmq_server

- Improved the `vmq-admin list session` command with a limit option controlling
  the returned number of sessions as well as the possibilty to customize which
  session data is returned.

- Change to plugin administration. To make VerneMQ configuration more consistent
  and simpler to configure it is now possible to configure plugins directly in
  the `vernemq.conf` file.
  
  - The `lib/vmq_plugin.conf` file is obsolete and no longer in use.
  - plugins are no longer persisted when enabled via the `vmq-admin` tool, but
    have to be added to the `vernemq.conf` file manually like any other
    configuration. An example looks like:
    
    `plugins.myplugin = on`
    `plugins.myplugin.path = /path/to/plugin`
    
    Configuration specific settings are then configured like this:
    
    `myplugin.setting = ...`

  - The above changes has the following impact on existing settings in the
    `vernemq.conf` file:
    - The `acl_file` setting is replaced by `vmq_acl.acl_file`.
    - The `acl_reload_interval` setting is replaced by
      `vmq_acl.acl_reload_interval`
    - The `password_file` setting is replaced by `vmq_passwd.password_file`
    - The `password_reload_interval` setting is replaced by
      `vmq_passwd.password_reload_interval`.
    - The `bridge` prefix has been replaced by the `vmq_bridge` prefix.
      
      Make sure to update the configuration file accordingly.

- Add implementation of shared subscriptions as specified in the MQTTv5 draft
  spec. Currently two different distribution policies are supported:

  - `prefer_local` which will, if possible, deliver the message to a random
    local subscriber in the shared subscription group, otherwise the message
    will be delivered to a random remote subscriber of the shared
    subscription (if any).
  - `random` will distribute the messages random between all members of the
    shared supscription regardless if they are local or are subscribed via
    another node.
  - `local_only` will consider only node-local members of the subscriber group
    for delivery.

  A message published to a shared subscription will first be delivered to online
  members of the shared subscription if any exist, failing that, the message
  will be delivered to an offline subscriber of the shared subscription. Due to
  the fact that the MQTTv5 spec is not yet final, this feature is marked as
  BETA.

  NOTE: To upgrade a live cluster all nodes must already be running 0.15.3 or
  newer! This feature is incompatible with older releases.

- Use of specific routing tables for non-wildcard topics. This improvement 
  results in faster routing table lookups for non-wildcard subscriptions, and
  reduces overall memory consumption of the routing tables.
  
### vmq_diversity

- Minor fixes and improvements.
- Add support for caching ACLs in lua scripts.
- Add support for bcrypt password hashes.


## VERNEMQ 0.15.3

### vmq_server

- Fix a bug in the retain messages fastpath code.
- Add the first version of the VerneMQ HTTP admin API (BETA).
- Fix a nodename change bug, fix contributed by @dcy.
- Fix an oversight preventing the client id from being overridable in `auth_on_register`.

### vmq_commons

- Fix a bug in the MQTT parser.

### vmq_passwd

- Make compatible with the OpenSSL 1.1 API.

## VERNEMQ 0.15.2

### vmq_server

- Fix a queue migration corner case with racing subscribers

## VERNEMQ 0.15.1

The VerneMQ dependencies vmq_server, vmq_acl, vmq_passwd, vmq_plugin,
vmq_commons were moved to the erlio/vernemq apps folder. The repos are kept
around for some time, and the issue trackers are moved to erlio/vernemq.

This release contains some backward-incompatibilities with 0.14.2. Get in touch
if professional support is required for upgrading. 

### vmq_server

- BC-break: New Subscriber format, the old format gets automatically translated 
    to the new format. However in a clustered setting nodes running the old
    format can not deal with the new format once those records are replicated.
- BC-break: More control over behaviour during netsplits:
    New configuration parameters 
    - `allow_register_during_netsplit`
    - `allow_subscribe_during_netsplit`
    - `allow_unsubscribe_during_netsplit`
    - `allow_publish_during_netsplit`
    obsolete the `trade_consistency` configuration parameter. Which was often
    misused together with `allow_multiple_sessions` in order to reach high
    availability. With the new configuration parameters the behaviour has to be
    explicitly configured. The old `trade_consistency` configuration parameter
    is not supported anymore.
- BC-break: `on_offline_message/1` hook replaced with `on_offline_message/5`
    The new hook includes the SubscriberId, QoS, Topic, Payload, and RetainFlag.
- Stability and performance improvements:
    In order to protect the broker from reconnect storms multiple processes
    implement the gen_server2 behaviour instead of gen_server. Moreover
    the queue subscriber is now load balanced to be able to cope with massive
    amounts of firing dead links. 
    Improved retain performance for subscriptions without wildcards
- Support for PROXY protocol
- TLS certificate verification depth can be specified in vernemq.conf
- The `max_message_size` configuration now acts on the MQTT frame level instead of
  the MQTT payload. This has the benefit of preventing that too big messages
  reach the MQTT state machine. However this now includes every MQTT frame not
  only the publish payload.
- TCP Socket options can now be configured on a session level by returning
  a `{tcp_opts, list()}` modifier in the `auth_on_register` hook.

### vmq_bridge

- New configuration format enables to use host strings instead of only IP
  addresses.

### plumtree

- Protect broadcast module from overloading

## VERNEMQ 0.14.2

### vmq_server

- SSL/TLS Refactorings:
    GCM cipher suites are now supported.
    The `support_elliptic_curve` option was removed. If your setup doesn't support
    the elliptic curve cipher suites you may do nothing, as the Erlang ssl
    application automatically ignores the unsupported cipher suites. As an
    alternative you may specify the supported ciphers in the `vernemq.conf` file.

- VerneMQ is now compatible with Erlang 19:
    As of the upcoming VerneMQ 0.15.0 we'll switch to Erlang 19 as the default
    version. Backward compatibility with Erlang 17 is likely to be removed.

- Bug fix in queue migration

## VERNEMQ 0.14.1

### vmq_server

- BC-break: Refactoring of queue migration mechanism:
    This change requires that all cluster nodes use 0.14.1.
    The refactoring fixes multiple issues with multiple clients that use the same
    client id when connecting to different cluster nodes. 
- Metrics overflow fix

### vmq_plugin

- Fix shutdown of plugins:
    Disabling of application plugins that also dynamically register module plugins 
    (like `vmq_diversity` or `vmq_webhooks`) will properly cleanup the `vmq_plugin.conf`.

### General

- RaspberryPI 32bit profile
    Use `make rpi32` target to build a VerneMQ release for RaspberryPI

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
