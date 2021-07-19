# Changelog

## VerneMQ 1.12.1

- Revert binary_to_term safe calls to bare version

## VerneMQ 1.12.0

- Fix bug causing the `pool_size` option for databases to not be respected.
- Update Hackney HTTP client to version 1.17.0
- Allow configuration for TCP listener buffer sizes in vmq_cluster_com module
- Autotune TCP 'buffer' for outgoing TCP connections in vmq_cluster_node module
- Fix command line tool to allow managing anonymous client sessions (Issue #1673)
- Allow custom option for HTTPS endpoints (WebHooks)
- Adds PEM validation of certificate files in server and webhooks schemas
- Adds a new CI profile to the rebar3
- Bumps MongoDB driver to latest
- Adds support for MongoDB DNS SRV lookup in `vmq_diversity`
- Adds authentication to MongoDB Lua test script in `vmq_diversity`
- Adds Docker Compose file for local testing
- Update `vmq_diversity` to newest Luerl version
- Bumps `rebar3` executable
- Update Hackney HTTP client to version 1.17.4
- Upgrade Cowboy dependency to 2.8.0
- Adds support for `auth_source` in MongoDB connections in `vmq_diversity`
- Enforce UTF8 strings in topics
- Use safe mode for binary_to_term in SWC
- Fix Proxy protocol handling for WebSocket listener.
- Updates in `vmq_swc` plugin to allow for unique SWC node ids, leading to fixes in synchronisation after
  a node leaves and re-joins a cluster.
- Adds `topic_max_depth` config value to enforce configurable global maximum number of allowed topic levels (`CVE-2021-33176`)
- Ensures that MQTT_v5_user_properties are stored to disk

## VerneMQ 1.11.0

- Improve Proxy protocol error logging (warn instead of crash process).
- Add retain command to vmq-admin usage output
- Bridge Plugin: Continue to publish during netsplit (that is, during cluster not_ready)
- Bridge Plugin: Make internal publish use the configured per-topic QoS
- Upgrade package `bcrypt` to fix compilation in OSX (#1500).
- Fix issue with loading status dashboard from behind a proxy with a basepath set
- Fix bug where the server would not parse multiple WebSocket protocols correctly.
- New feature: Allow configuration of allowed ECC named curves in configuration file. (thanks to @varnerac)
- New feature: Add `on_topic_unsubscribe` hook (PR #1539, Issue #1326). (thanks to @khodzha)
- Update to jquery 3.5.1 and bootstrap 4.1.2 in status page.
- Add case clause for proxy protocol 'local command', causing unnecessary error logging in loadbalancer health checks.

## VerneMQ 1.10.4.1

Patch Release to:
- Fix the new bridge metrics labels that prevented the Prometheus metrics to be exported correctly (blocking Prometheus export entirely).

## VerneMQ 1.10.4

- Improve buffering in in-memory queue of outgoing bridges.
- Add a Name to bridges, so that a bridge can be identified by {Name, Host, Port}, instead of {Host, Port} only. This allows multiple bridges to the same remote endpoint.
- Add bridge Name and MQTT client mailbox size to `vmq-admin bridge show` command.
- Add per QoS/per bridge in and out counters to pull resp. push bridges.
- Log bridge connection setups and subscribes (info level).
- Support Power Linux LE (ppc64le) platform.
- Add `on_session_expired/1` hook to `vmq_webhooks` schema.
- Add Subscriber trie/event handler readiness check to handle fast subscribers after a reboot (race condition, #1557).
- Update nodetool escript with latest version.
- Fix internal Watermark update in SWC (#1556).
- Handle LevelDB truncated record corruption more automatically in LevelDB SWC and Messagestore backends.
- Catch ETS table call in `vmq_cluster:netsplit_statistics/0` (#1537).
- Add compatibility with [Erlang/OTP 23](http://blog.erlang.org/OTP-23-Highlights/). This release requires Erlang/OTP 21.3 or later.
- Upgrade package `bcrypt` to version 1.1.0.
- Upgrade package `hackney` to version 1.16.0 (dependencies of `hackney` were updated as well).
- Fix to allow equal signs on parameter values on `vmq-admin` commands (#740).

 URL parameters on webhooks

## VerneMQ 1.10.3

- Add new `on_session_expired/1` hook to `vmq_diversity` and `vmq_webhooks`.
- Add datetime prefix to the tracer output. The datetimes are expressed in UTC with
 [iso-8601](https://www.w3.org/TR/NOTE-datetime) format (eg '2020-04-29T21:19:39Z'). (#782)
- Add start command to Bridge CLI.
- Fix options passed to PublishFun for plugins (#1516)
- Add configurable connect time (mqtt_connect_timeout) between establishing the connection and sending CONNECT (#735, #824).
- Fix tracing error when using payload modifiers (#1517).
- Make `vmq-admin bridge show` command more robust against bridge client state machine timeouts (#1515).
- Fix vmq health http format error (#1529).
- Add new CLI output formatter (#1502).
- Remove minor functions for OTP 18. OTP 18 is not supported anymore (#1523).
- Update VerneMQ Schema info on SWC.
- Add Inet6 support to the vmq_diversity MySQL plugin (#1461).
- Improve error message when plugin chains have exhausted (#1465).
- Upgrade clique dependency to fix bug with empty KV params.

## VerneMQ 1.10.2

- Support multilevel bridge prefixes.
- Make SSL cert depth configurable for bridges.
- Fix directory paths for metadata backend for RocksDB and Leveled.
- Fix number only client IDs in vmq-admin session CLI (#1436).
- Fix proxy protocol handling bug for WebSockets listeners (#1429).
- Fix compilation for OS X.
- Fix bug in vmq_swc where a cluster leave didn't properly cleanup the node clock.

## VerneMQ 1.10.1

- Upgrade the Plumtree metadata backend to include a fix for a crash which could
  occur during metadata exchange with a remote node if the remote node becomes
  unavailable.
- Fix bug where vmq_metrics crashes because external metric providers haven't
  started yet.
- Fix compilation on OSX Mojave and Catalina. Requires running `brew install openssl snappy`.

## VerneMQ 1.10.0

- Fix bug where websocket MQTT clients that didn't provide a
  `sec-websocket-protocol` header to cause a crash to be logged. This case is
  now handled and the connection is terminated gracefully (#1317).
- Fix bug which caused clients MQTT bridges using protocol versions (131 and
  132) to be unable to connect (#1306).
- Upgrade the `hackney` so VerneMQ is compatible with a TLS change in Erlang/OTP
  22.1.
- Fix bug preventing MQTT 5 publish in/out counts from being shown on the HTTP
  status page.
- Fix lager issue on Raspberry Pi preventing VerneMQ from starting (#1305).
- Upgrade `epgsql` dependency to version 4.3.0 to get better error messages
  (#1336).
- Add new `max_last_will_delay` value `client` which means the value from the
  client is used instead of being overriden by the broker. This `client` value
  is now the default. This setting only applies to MQTT 5.0 clients.
- Add hidden option `response_timeout` to `vmq_webhooks` endpoints. With this
  the time `vmq_webhooks` waits for a response from the remote endpoint can be
  configured. Default is 5000 milliseconds.
- Add new `on_deliver/6` and `on_deliver_m5/7` hooks which adds QoS and retained
  information to the parameters. The old variants are deprecated and will be
  removed in the next major version.
- Add new `on_session_expired/1` hook which is called when an offline session
  expires and the state is deleted.
- Handle CRL PEM entries with certificates containing empty revocation lists
  (#1337).
- Fix typo in `vmq-admin listener start` command (#1348).
- Optimize subscription performance when many retained messages are stored on
  the broker and the subscription contains wildcards.
- Fix bug where MQTT 5 publish expiry interval was lost when writing the message
  to the message store.
- Fix bug where a retried MQTT publish after resuming an offline session used
  the wrong message id.
- Fix MQTT 5 shared subscription bug where writing the message to the message
  store resulted in a corrupt message record once a offline session was resumed.
- Refactor built-in message storage to enable multiple storage engines as well as
  to streamline the implementation of alternative message storage plugins.
- Fix metric description string in vmq_bridge.
- Fix regression in vmq_bridge where the retain bit wasn't handled properly for
  incoming publishes.
- Enable to define the MQTT protocol version to be used by vmq_bridge (4 or 3).
- Fix vmq_bridge dynamic configuration handling.
- Fix multiple minor issues in vmq_swc.
- Fix HTTP handlers to support parameterized media types.
- Upgrade lager, poolboy, jsx, luerl dependency.

## VerneMQ 1.9.2

- Fix bug causing idle websocket connections to be closed after 60 seconds
  (#1292).
- Fix MQTT 5.0 bug causing LWT not being sent when client disconnected with
  Disconnect with Will Message (0x04) (#1291).
- Ensure MQTT 5.0 subscription identifers are added to messages delivered via
  shared subscriptions (#1294).
- Fix protocol version check bug which would allow MQTT 5.0 clients to connect
  even if the MTT 5.0 protocol was not specified for the listener. This happened
  for clients which connected with an empty client-id.
- Fix bug in `vmq_diversity` and `vmq_webhooks` where the plugin hooks would be
  registered out of order, even though the plugins themselves would be started
  in the correct order (#1295).

## VerneMQ 1.9.1

- Update `cuttlefish` to fix parse issue with lines in the `vernemq.conf` file
  consisting solely of white-space (#1208).
- Fix bug in the `vmq_webhooks` `auth_on_subscribe_m5` and `on_subscribe_m5`
  hooks (#1280).
- Fix issue where errors would be logged when the /health endpoint was called
  (#1281).
- Ensure MQTT 5.0 subscription identifiers are added to retained messages
  delivered to the client when the subscription is made (#1287).
- Fix bug in `vmq_swc` which could prevent VerneMQ from starting up.

## VerneMQ 1.9.0

- Ensure mountpoints specified at the protocol level are inherited on the
  specific listeners.
- Fix missing output from `vernemq version` (#1190).
- Ensure errors from the parser variable decode are handled correctly and logged
  at the debug level instead of causing a crash in the socket process.
- Handle list of `sec-websocket-protocol` correctly (#1149) in the websocket
  implementation.
- Ensure that plumtree application isn't started when metadata plugin SWC is used.
- Fix bug preventing restarting Lua states properly in case of a crashing script.
- Fix warnings due to deprecated lager configuration (#1209).
- Upgrade `lager` to version *3.7.0* (In preparation for OTP 22 support).
- Make VerneMQ run on Erlang/OTP 22.
- Add new webhooks CLI register flag `--no_payload=true` which, if enabled, will
  remove the MQTT payload from the JSON object send via the `auth_on_publish`
  and `auth_on_publish_m5` payloads. The flag can also be set in the config file
  using `vmq_webhooks.hookname.no_payload=on`.
- Add metric `client_keepalive_expired` which tracks clients that failed to
  communicate within the keepalive time.
- Expose the `crypto:hash/1` function in LUA, for example to do a 256bit sha3
  hash one can call `crypto.hash("sha3_256", data)`. The hashing algorithms
  supported are exactly the ones supported by the `crypto:hash/1` function and
  may change depending on the Erlang/OTP version used to build Erlang. See the
  complete list of supported hashes here:
  http://erlang.org/doc/man/crypto.html#Digests%20and%20hash. If passed an
  unknown hashing algorithm an error is raised.
- Fix prefix handling in the bridge plugin (vmq_bridge).
- Strengthen parameter validation in the `bcrypt.hashpw/2` LUA function in
  `vmq_diversity`.
- Pass arguments correctly to `vmq-admin` when called via `sudo`.
- Handle rate metric labels correctly in the example grafana dashboard.
- Upgrade cowboy to version 2.6.3 as well as cowlib and ranch to versions 2.7.3
  and 1.7.1 respectively. This update also means the PROXY protocol code was
  removed and the PROXY support from cowboy is used instead.
- Handle retained flag in the bridge plugin (vmq_bridge).
- Ensure systemd doesn't terminate VerneMQ if it is slow to start.
- Implement bridge protocol handling (protocol version 131) for bridges
  connecting to VerneMQ such that the retained bit is kept on messages routed to
  the bridge (Retained as Publish) and messages coming from the bridge are not
  forwarded to the bridge if it has a matching description (No Local).
- Fix message store startup consistency checking routine which resulted in
  potentially deleting wrong messages in cases where an inconsistency was detected.
  This fix also increases message storage performance during startup as well as
  during normal operation. However, the nature of the bug and the provided solution
  don't enable a simple downgrade path and introduces a backward incompatibility if
  a VerneMQ message store containing offline messages has to be downgraded to 1.8.0
  or earlier versions.
- Upgrade dependency `cuttlefish` to version *2.2.0*.
- Improve large queue initialization performance by reducing algorithmic
  complexity from O(n^2) to O(nlogn) where n is the number of offline messages.
- Add the ability to modify the `username` on `auth_on_register` and `auth_on_register_m5`
  hooks. Supports both `vmq_diversity` and `vmq_webhooks`.
- Upgrade the `vmq_diversity` redis driver `eredis` to version 1.2.0.
- Fix `vmq_diversity` supervisor restart shutdown issue (#1241).

## VerneMQ 1.8.0

- Cleanup cluster state information on a node which is being gracefully removed
  from the cluster so if it is restarted it comes back up as a stand-alone node
  and won't try to reconnect to the remaining nodes.
- The `allow_multiple_sessions` option is deprecated and will be removed in
  VerneMQ 2.0.
- Add new `coordinated_registrations` config option which allows for faster
  uncoordinated registrations (replaces side-effect of
  `allow_multiple_sessions`).
- Upgraded dependency `swc`. Improves memory and cpu usage by fixing node clock
  update bug.
- Set queue expiration (`persistent_client_expiration`) timer on queues after a
  broker restart (#1071).
- Fix status page to show all cluster nodes (#1066).
- Fix Lua `json.decode()` when decoding empty Json objects (#1080).
- Fix `vmq_diversity` cache invalidation timing issue when a client reconnects
  right after a disconnect (#996).
- Fix retry of MQTT publish frame in `vmq_bridge` (#1084).
- Fix outgoing qos2 message id bug (#1045).
- VerneMQ now requires Erlang/OTP 21.2 or newer to build.
- Use `atomics` and `persistent_term` to handle metrics instead of using the
  `mzmetrics` library which has been removed.
- Change `vmq_reg:publish/4` to return `{ok, {NumLocalMatches, NumRemoteMatchs}}`
  instead of `ok`. This introduces a breaking change to the *unofficial* publish
  API provided by the `vmq_reg:direct_publish_exports/1`.
- Add the `router_matches_local` and `router_matches_remote` counter metrics,
  counting the total number of matched local and remote subscriptions.
- Add a *routing score* to every cluster node listed on the VerneMQ status page.
  The routing score consists of two percentages similar to `75 / 25` indicating
  that 75% of the received MQTT publish frames were routed to local subscribers
  and 25% were forwarded to subscribers on different cluster nodes. The routing
  score can be used to detect imbalances in a VerneMQ cluster.
- The MQTT 5.0 plugin hook definitions have been updated as follows:
  - The `Payload` and `Properties` arguments to the `on_deliver_m5` hook have
    been swapped to be consistent with all the other `_m5` hooks.
  - property modifiers are now using the same names as the properties in the
    incoming properties, for example the `user_property` is now
    `p_user_property`.
  - Property modifiers have been moved into a `properties` modifier map. So for
    example instead of returning `{ok, #{p_user_property => ...}}` one now
    returns `{ok, #{property => #{p_user_property => ...}}}`. This change makes
    it possible to drop properties from a plugin hook.
  - In the `vmq_webhooks` plugin property modifiers have been moved into a
    separate JSON object with the key `properties` inside the modifiers JSON
    object. The `vmq_webhooks`.
  - The `{ok, binary()}` return clause has been removed from the `on_deliver_m5`
    hook, use the modifier map (`{ok, Modifiers}`) instead.
  - The `{ok, binary()}` return clause has been removed from the
    `auth_on_publish_m5` hook, use the modifier map (`{ok, Modifiers}`) instead.
  - Note, MQTT 5.0 support in VerneMQ is still in Beta.
- Fix error when tracing clients connecting with a LWT.
- Add file validation to check if files in the `vernemq.conf` exist and are
  readable.
- Fix that disconnects due to client migrations were logged as warnings. They
  are now treated as the normal termination case.
- Improve MQTT 5.0 support in the `vmq_webhooks` plugin: support more properties
  and improve the tests.
- Instead of using the `node_package` dependency to build binary packages, the
  external Ruby command line tool `fpm` is used. This enables cleaning up the
  Makefile as well as the upgrade of the `rebar3` build tool.
- Improve MQTT 5.0 support in the `vmq_diversity` plugin: support all MQTT 5.0
  hooks in Lua and more properties.
- Support TLS encrypted connections to PostgreSQL when using
  `vmq_diversity`. Fixes #811.
- Support TLS encrypted connections to MongoDB when using `vmq_diversity`.
- Add CockroachDB support to the `vmq_diversity` plugin.
- Make it possible to configure how many concurrent bcrypt operations are
  possible via the `vmq_bcrypt.pool_size` config.
- Fix incorrect format string in shared subscription debug log.
- Upgrade `hackney` to version 1.15.1 (dependencies of `hackney` were updated as
  well).
- Fix bug which could happen while repairing dead queues in case the sync
  request fails.
- It is now possible to configure TCP send and receive buffer and user-level
  buffer sizes directly on the MQTT listeners or protocol levels (currently only
  TCP and TLS listeners).
- Add the `socket_close_timeout` metric. The metric counts the number of times
  VerneMQ hasn't received the MQTT CONNECT frame on time and therefore forcefully
  closed the socket.
- Fix the potential case of late arriving `close_timeout` messages triggered
  by the `vmq_mqtt_pre_init`.
- Change log level from `debug` to `warning` when VerneMQ forcefully terminates a
  MQTT session FSM because of an unexpected message.
- Fix `vmq_diversity` ACL rule hash collision issue (#1164).
- Improve performance of shared subscriptions when using many subscribers.
- Add the `vmq_pulse` plugin. This plugin periodically sends cluster information to
  a pulse backend via HTTP(S). The plugin is NOT enabled by default, and once
  enabled it has to be setup with `vmq-admin pulse setup`. This enables a support
  crew to analyze the cluster performance without having direct access to the VerneMQ
  nodes. Such a 'Pulse' contains the following information:
  - Cluster status, similar to `vmq-admin cluster show`
  - Plugins, similar to `vmq-admin plugin show --internal`
  - Metrics, similar to `vmq-admin metrics show`
  - Names and versions of loaded OTP applications
  - Erlang System Version
  - OS Kernel information, output of `uname -a`
  - CPU, RAM, and disk usage
  - Pulse Version
  Note: The `vmq_pulse` plugin is in Beta.
- Fix the systemd service unit file to set `LimitNOFILE=infinity`. This enables VerneMQ
  to request as many file descriptors as configured for the `vernemq` user.

## VerneMQ 1.7.0

- Fix `vmq_webhooks` issue where MQTTv5 hooks where not configurable in the
  `vernemq.conf` file.
- Support shared subscriptions in `vmq_bridge`.
- Fix bug in Prometheus output (#923).
- Fix `max_message_rate` to include MQTTv5 sessions.
- Support new `throttle` modifier for the `auth_on_publish` and
  `auth_on_publish_m5` hooks which will throttle the publishing client by
  waiting for a given number of milliseconds before reading new data from the
  client socket. Note, backpressure is not supported for websocket
  connections. This feature was kindly sponsored by Arduino SA
  (https://www.arduino.cc/).
- Fix issue with long-running `vmq-admin` commands (#644).
- Added a new HTTP module `vmq_health_http` exposing a `/health` endpoint that
  returns **200** when VerneMQ is accepting connections and joined the cluster
  (for clustered setups) or returns **503** in case any of the two conditions
  are not met (#889).
- Fix issue where a QoS2 message would be republished if a client would resend
  the PUBLISH packet with the same message id within a non-finished QoS2 flow
  (#944).
- Make VerneMQ build on FreeBSD (11.2-RELEASE) with `gmake rel`.
- Fix issue with blocking socket commands in inter-node communication.
- Improve error messages when parsing invalid CONNECT packets.
- Log IP and port information on authentication failures to make it easy to
  block the client with tools like Fail2ban (#931).
- Fix issue which would crash the session if a client would resend a (valid)
  pubrec during a Qos2 flow (#926).
- Fix `vmq_diversity` error message if invalid or unknown modifiers are returned
  from a lua hook implementation.
- Fix issue preventing MQTT 5.0 properties from being modifiable in
  `auth_on_publish_m5` (#964).
- Fix issue which causes a crash if not enough data is available while parsing
  the CONNECT frame in `vmq_mqtt_pre_init` (#950, #962).
- Fix issue which could cause `vmq-admin session show` to crash when using
  `--limit=X` to limit the number of returned results (#902).
- Handle edge case in the websocket implementation which could cause warninigs
  when the session was terminating.
- Expose `vernemq_dev_api:disconnect_by_subscriber_id/2` in lua
  `vmq_api.disconnect_by_subscriber_id/2`, an example looks like:
  `vmq_api.disconnect_by_subscriber_id({mountpoint = "", client_id =
  "client-id"}, {do_cleanup = true})`.
- Improve `vmq_webhooks` errors so the error from the endpoint is shown as the
  error reason where relevant.
- Optimization: optimize away read operation during client registration by
  reusing already available data.
- Enable plugins to expose metrics via the available metric providers.
- Add histogram metric type.
- Fix incorrect debug log message in QoS 2 flow.
- Expose bridge metric for messages dropped in case the outgoing queue is full.
- Log (debug level) when a LWT is suppressed on session takeover.
- Optimize subscribe operation by refactoring away one read from the metadata
  store.
- Add protection mechanism for the plumtree metadata store from subscription
  floods by dropping and not acknowledging `i_have` messages when the mailbox
  size grows above a certain threshold. This threshold is configurable via the
  `plumtree.drop_i_have_threshold` hidden option. The default is 1000000. This
  work was kindly contributed by ADB (https://www.adbglobal.com).
- Add new `vmq-admin retain` commands to inspect the retained message store.
- Support for different MySQL password hashing methods in `vmq_diversity`,
  ensuring compatibility with MySQL 8.0.11+. The method is configurable via the
  `vmq_diversity.mysql.password_hash_method` option which allows:
  `password` (default for compatibility), `md5`, `sha1` or `sha256`.
- Fix the HTTP `/status.json` endpoint to have a valid JSON output (#786).
- Fix bug where internal application plugins where shown as normal plugins.
- Fix crash in bridge when calling `vmq-admin session show` by fixing the
  `vmq_ql` row initializer to handle plugin sessions (the bridge starts a local
  plugin session).
- Add improvements to VerneMQ status page to have nicer UI and be readable on smaller devices.
- Upgraded dependency `sext` to 1.5.0 (required for better OSX compilation).
- Do not accept new client connections while the broker is shutting down as this
  could cause errors to be reported in the log (#1004).
- Fix `vmq_diversity` PostgreSQL reconnect issue (#1008).
- Fix `vmq_webhooks` so peer port is not considered when caching the
  `auth_on_register` to avoid cache misses.
- Multiple bug fixes and improvements in `vmq_swc`.
- Add the `vmq_swc` metadata plugin (using the already existing LevelDB backend) to
  the default VerneMQ release. To use `vmq_swc` instead of `vmq_plumtree` set
  `metadata_plugin = vmq_swc` in `vernemq.conf`. `vmq_swc` is still in Beta.
- Add missing increments of the `mqtt_connack_sent` metric for CONNACK
  success(0) for MQTT 3.1.1.
- Handle edge case with unknown task completion messages in `vmq_reg_sync` after
  a restart.
- Fix bug which could cause a queue cleanup to block indefinitely and cause the
  `vmq_in_order_delivery_SUITE` tests to fail.
- Add a new metric (queue_initialized_from_storage) to better monitor queue
  initialization process after a node restart.
- Fix edge case where an extra queue process could be started when metadata
  events arrive late. Now local queue processes are only started when triggered
  via a new local MQTT session.
- Reimplement dead queue repair mechanism.
- Add feature to reauthorize existing client subscriptions by reapplying current `auth_on_subscribe`
  and `auth_on_subscribe_m5` hooks. This feature is exposed as a developer API in `vernemq_dev`, via
  the `vmq-admin session reauthorize` CLI command, and as a API for `vmq_diversity` Lua scripts. This
  work was kindly sponsored by AppModule AG (http://www.appmodule.net).
- Improve planned cluster leave queue migration speed significantly (#766).
- Start metrics server before setting up queues to ensure queue metric counts
  are correct when restarting VerneMQ.
- Let Travis CI build the VerneMQ release packages.
- Add new metric `system_process_count` which is a gauge representing the
  current number of Erlang processes.
- Add `queue_started_at` and `session_started_at` information to the `vmq-admin
  session show` command. Times are POSIX time in milliseconds and local to the
  node where the session or queue was started.

## VerneMQ 1.6.0

- Fix issue when calling a function in a Lua script that requires more time to complete than the default `gen_server` timeout (#589).
- Silently drop messages published by the client that use a routing key starting with '$'.
- Full MQTTv5 support

  With this release VerneMQ officially supports MQTT protocol version
  5.0. The MQTTv5 support is currently marked as in BETA and we may
  still have to change some things.

  VerneMQ has full support for the complete MQTTv5 spec, but to list a
  few of the new features:

  - Support for enhanced (re)authentication
  - User properties
  - Message Expiration
  - Last Will and Testament delay
  - Shared subscriptions
  - Retained messages
  - Request/Response flows
  - Topic aliases

    VerneMQ supports topic aliases from both the client to the broker and from
    the broker to the client.

    When a client connects to the broker, the broker will inform the client of
    the maximum allowed topic alias using the topic alias max property on the
    CONNACK packet (if it has been set to a non-zero value). The topic alias
    maximum property can be configured through the `topic_alias_max`
    configuration variable or overriden in a plugin in the `auth_on_register`
    hook. The broker will then handle topic aliases from the client as per the
    MQTTv5 spec.

  - Flow control
  - Subscription flags Retain as Published, No Local and Retain Handling.
  - Subscriber ids

  By default MQTTv5 is disabled, but can be enabled on a listener basis, for
  example `listener.tcp.allowed_protocol_versions=3,4,5` would enable MQTT
  version 3.1, 3.1.1 and 5.0 on the TCP listener.

  MQTTv5 support has been added to the `vmq_passwd`, `vmq_acl`, `vmq_webhooks`
  plugins as well as the parts of the `vmq_diversity` plugin to support the
  MySQL, PostgreSQL, Redis and MongoDB authentication and authorization
  mechanisms. The Lua scripting language provided by the `vmq_diversity` plugin
  does not yet expose all MQTT 5.0 plugin hooks.

  The `vmq_bridge` plugin currently has no support for MQTTv5.

  !! Note !! that all MQTTv5 related features and plugins are in BETA and may
  still change if needed.

- The metrics:

    mqtt_connack_not_authorized_sent
    mqtt_connack_bad_credentials_sent
    mqtt_connack_server_unavailable_sent
    mqtt_connack_identifier_rejected_sent
    mqtt_connack_unacceptable_protocol_sent
    mqtt_connack_accepted_sent

  Have been merged into a single metric `mqtt_connack_sent` and the various MQTT
  3.1.1 return codes have been mapped into labels. So for example
  `mqtt_connack_sent` with the label `return_code=success` replaces
  `mqtt_connack_accepted_sent`.

- Added on_message_drop hook that is called for every message dropped due to
  exceeding the MQTTv5 max_packet_size property, hitting the message expiry,
  or when load shedding when enqueing messages.
- Fix ordering bug in webhook subscribe topic authentication and topic rewrites
  (#823)
- Fix issue when terminating the `vmq_server` application (#828).
- Make VerneMQ build on SmartOS / Illumos / Solaris.
- Ensure strings passed from Lua to the logger are escaped (#864).
- Handle Mongo Date / ISODate datatype properly in Lua / vmq_diversity
  (#857).
- Optimize subscribe/unsubscribe operations for large fanout cases.
- Allow non-standard MQTT version 131 (MQTT 3.1 bridge) by default (this was
  accidentally changed in VerneMQ 1.4.0).
- Improve error messages returned by the `vmq_diversity` plugin so it's easier
  to understand and debug authentication and authorisation issues.
- Improve performance for looking up queue processes.
- Make VerneMQ run on Erlang/OTP 21. To do this we had to upgrade `rebar3` to
  version 3.6.0 and `lager` to version 3.6.3. We also removed the `eper`
  dependency and suppressed some new warnings due to `erlang:get_stacktrace/0`
  having been deprecated in Erlang/OTP 21.
- The bridge plugin (`vmq_bridge`) now has an option to queue outgoing message
  in case the network connection disappears. This is configurable with the
  `max_outgoing_buffered_messages` setting on a bridge. In addition the bridge
  now has a simple cli interface under `vmq-admin bridge show`. This work was
  kindly sponsored by Diacare-Soft(http://diacare-soft.ru).
- Fix multiple message retry issues in the MQTT client used by vmq_bridge.
- Fix issue where the message ordering wasn't preserved after a client reconnect.
- Add experimental `vmq_swc` plugin that provides an alternative to the existing
  `vmq_plumtree` for metadata storage and replication. One must compile VerneMQ
  with `make swc` to generate a release containing the `vmq_swc` plugin.
- Remove unused `vmq_bridge` ssl `capath` config option. This was never used
  internally.
- Upgrade `lager` to version 3.6.8 to fix build issue on Mac OS X Mojave.

## VerneMQ 1.5.0

- Fix issue in the bridge preventing it from reconnecting after a connection
  timeout (#726).
- Fix issue with self referential cluster leaves and cluster readiness probing
  in general (#717).
- Add preparations for MQTTv5:
  - Make it possible to extend the cluster internal message format when adding
    new features while ensuring back- and forwards compatibility.
  - Refactor the leveldb message store code to support versioned indexes as well
    as messages.
  - Refactor the retained message store to support versions.
  - Add support for subscriber data with subscription options.
- Fix issue when validating a shared subscription topic (#756).
- Fix issue with retried PUBREC frames (#750).
- Make it possible to suppress the Last Will and Testament message if an
  existing session already exists for the connecting client. This is
  configurable via the hidden setting `suppress_lwt_on_session_takeover` in the
  VerneMQ configuration file. The default is `off`.
- Fix issue with PUBREL frames retried after client reconnects (#762).
- Refactor and cleanup retry mechanism.
- Warn about deprecated setting `message_size_limit` only when it has been defined.
- Change build script so nightly packages will have names on the form
  `vernemq_1.4.0-10-gde1b1f5-1_amd64.deb`, where `de1b1f5` refers to the commit
  hash from which the package was built and `10` is the number of commits since
  the latest tag. This makes it much easier to reason about nightly builds and
  the features they contain.
- Refactor and generalize the metrics systems to allow labelling metrics coming
  from different sources in order to differentiate them in the various exporters
  and the command line. Labels are added to the Prometheus exporter by
  default. To enable generating additional metrics from the labels in the
  graphite exporter the hidden setting `graphite_include_labels` has to be set
  to `on` in the `vernemq.conf` file. Labels are not exposed in the $SYS
  metrics.
- Ensure the `vmq_bridge` is properly restarted after a crash (#785).
- Fix issue where not calling `http.body(ref)` in a Lua script would not return
  the underlyning HTTP connection to the connection pool. This now happens
  automatically, irregardless of the user calling `http.body(ref)` or not
  (#588).

## VerneMQ 1.4.0

- Fix for OSX compilation issue on `vmq_passwd` due to openssl headers not found.
- Refactoring to further abstract and generalize the Plumtree metadata layer.
- Small refactoring moving the calling of plugin hooks into the fsm code. This
  is a preparation for MQTTv5. Note, this change has an impact on the
  `vmq_reg:direct_plugin_exports/1` function. Even though this function is
  internal and as such not guaranteed to be stable, we know some plugin
  developers use it and we therefore wanted to describe the changes. It works as
  before, **except** that the hooks `auth_on_subscribe`, `on_subscribe` or
  `on_unsuscribe` hooks will no longer be called when using the exported
  functions.
- Corrected a few spelling errors in the `vmq-admin` help commands.
- Added two new hidden configuration parameters for `vmq_webhooks`. `vmq_webhooks` uses
  the [hackney HTTP client](https://github.com/benoitc/hackney) for HTTP requests.
  It starts its own hackney pool with a default connection pool of 100, and a
  connection timeout of 60000 milliseconds. This connection pool con now be
  configurable using the settings `vmq_webhooks.pool_max_connections` and
  `vmq_webhooks.pool_timeout` (#621).
- Add a first version of a VerneMQ status page showing node and cluster
  information. This web-page is by default available on
  http://localhost:8888/status. The status page is implemented using jQuery,
  Twitter Bootstrap und mustache.js. As this is the first version the page
  should be considered experimental and will likely be changed in future
  releases.
- Bugfix: Handle return values correctly when enqueuing messages to offline
  shared subscribers (#625).
- Bugfix: Fix issue preventing messages delivered to a subscriber group from
  being delivered to the online group members first before attempting delivery
  to offline queues (#618).
- Fix some Dialyzer issues.
- Reduce replication load during a netsplit by making sure data is not attempted
  to be replicated to unreachable nodes.
- Bugfix: Fix issue causing some `session show` options (`peer_host`,
  `peer_port`) to not work for websocket clients (#542).
- Bugfix: Fix routing table initialization issue after restarting a node
  preventing shared subscriptions on a remote node from being included in the
  routing table (#595).
- Bugfix: Fix race condition when fetching data from the internal query subsystem.
- Fix build issue on Raspberry PI (`make rpi-32`).
- Make it possible to specify which protocol versions are allowed on an MQTT
  listener. By default the protocol versions allowed are versions 3 and 4 (MQTT
  v3.1 and v3.1.1 respectively). To set the allowed protocol versions one can
  use `listener.tcp.allowed_protocol_versions = [3,4]` on the transport level or
  for a specific listener using
  `listener.tcp.specific_listener.allowed_protocol_versions`.
- Fix bug causing an exception to be thrown when `vmq-admin cluster leave` is
  used with a timout value less than 5 seconds (#642).
- Small refactoring enabling to store versioned message store values. This is a
  preparation for MQTTv5.
- Bugfix: Fix a bug that prevented user plugins with an explicit path to be
  loaded.
- Fix issue with new rebar3 upstream plugin version (the port-compiler) which
  made builds fail by pegging it to an older version (1.8.0).
- Add `xmerl` and `inets` from the Erlang standard library to the release in order
  to allow plugin developers to have these libraries available.
- Bugfix: Fix typo (`graphie_api_key` -> `graphite_api_key`) preventing the
  graphite api key from being set in the `vernemq.conf` file.
- Bugfix: WebHooks Plugin. Close the Hackney CRef so that the socket is given
  back to the Hackney pool, for the case of non-200 HTTP OK status codes.
- Bugfix: fix bug where queries with mountpoint and client-ids would return no
  entries causing disconnecting a client with a specific mountpoint to
  fail. Also ensure to use the default mountpoint if no mountpoint was
  passed. (#714).
- Upgraded vernemq_dev to include the `disconnect_by_subscriber_id/2` API.

## VerneMQ 1.3.0

- Add `proxy_protocol_use_cn_as_username` feature which for `proxy_protocol`
  enabled listeners enable or disable using the common name forwarded by the
  PROXY protocol instead of the MQTT username. To ensure backward compatibility
  this setting is enabled by default. This feature was kindly contributed by
  SoftAtHome (https://softathome.com/).
- New `vmq-admin` command to forcefully disconnect and cleanup sessions.
- Fix issue preventing ssl settings being inheritable on the listener level
  (#583).
- Fix issue where enqueuing data to a queue on a remote cluster node could cause
  the calling process to be blocked for a long time in case of the remote
  cluster node being overloaded or if a net-split has occurred.

  This issue can occur while delivering a shared subscriber message to a remote
  subscriber in the cluster (blocking the publishing client) or when migrating
  queue data to to another node in the cluster. In the case of shared
  subscribers a new (hidden) configuration parameter
  (`shared_subscription_timeout_action`) has been added which decides which
  action to take if enqueuing on a remote note times out waiting for the
  receiving node to acknowledge the message. The possibilities are to either
  `ignore` the timeout or `requeue` the message.  Ignoring the timeout can
  potentially lead to losing the message if the message was still in flight
  between the two nodes and the connection was lost due to a
  net-split. Requeueing may lead to the same message being delivered twice if
  the original client received the message, but the acknowledgement was
  lost. This fix was kindly contributed by SoftAtHome (https://softathome.com/).
- Fix typo in configuration name `plumtree.outstandind_limit` should be
  `plumtree.outstanding_limit`.
- Make the `vmq_cluster_node` processes able to handle system messages to make
  it easier inspect the process behaviour at run-time.
- Fix bug preventing `use_identity_as_username` from working on WSS sockets
  (#563).

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
