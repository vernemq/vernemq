# VerneMQ: A Distributed MQTT Broker

![docker-push][ci-workflow-badge]
![tests][test-workflow-badge]

![VerneMQ Logo](https://i.imgur.com/bln3fK3.jpg)

This project is a fork of the awesome ❤️  [MQTT broker](https://github.com/vernemq/vernemq) of the same name. We wanted to add some functionalities that we think can be useful for specific use-cases.

VerneMQ is a high-performance, distributed MQTT message broker. It scales
horizontally and vertically on commodity hardware to support a high number of
concurrent publishers and consumers while maintaining low latency and fault
tolerance. VerneMQ is the reliable message hub for your IoT platform or smart
products.

VerneMQ is an Apache2 licensed distributed [MQTT](http://www.mqtt.org) broker,
developed in [Erlang](http://www.erlang.org).

MQTT used to stand for MQ Telemetry Transport, but it no longer is an
acronym. It is an extremely simple and lightweight publish/subscribe messaging
protocol, that was invented at IBM and Arcom (now Eurotech) to connect
restricted devices in low bandwidth, high-latency or unreliable networks.

VerneMQ implements the MQTT 3.1, 3.1.1 and 5.0 specifications. Currently the
following features are implemented and delivered as part of VerneMQ:

On top of the following core VerneMQ features, this fork provides:
* Redis based subscription store
* TCP based events relay hook
* JWT based authentication
* Enhanced ACL

Core VerneMQ features:
* QoS 0, QoS 1, QoS 2
* Basic Authentication and Authorization
* Bridge Support
* $SYS Tree for monitoring and reporting
* TLS (SSL) Encryption
* Websockets Support
* Cluster Support
* Logging (Console, Files, Syslog)
* Reporting to Graphite
* Extensible Plugin architecture
* Multiple Sessions per ClientId
* Session Balancing
* Shared subscriptions
* Message load regulation
* Message load shedding (for system protection)
* Offline Message Storage (based on LevelDB)
* Queue can handle messages FIFO or LIFO style.
* MongoDB auth & integration
* Redis auth & integration
* MySQL auth & integration
* PostgreSQL auth & integration
* CockroachDB auth & integration
* Memcached integration
* HTTP Webhooks
* PROXY Protocol v2
* Administration HTTP API
* Real-time MQTT session tracing
* Full multitenancy
* Cluster status web page

The following features are also applies to MQTT 5.0 clients:

* Enhanced authentication schemes (AUTH)
* Message expiration
* Last Will and Testament delay
* Shared subscriptions
* Request/response flow
* Topic aliases
* Flow control
* Subscription flags (Retain as Published, No Local, Retain Handling)
* Subscriber identifiers
* All property types are supported: user properties, reason strings, content types etc.

## Quick Start

This section assumes that you have a copy of the VerneMQ source tree. To get
started, you need to first build VerneMQ.

### Building VerneMQ

Note: VerneMQ requires Erlang/OTP 21.2 or newer and `libsnappy-dev` installed in your system.

Assuming you have a working Erlang installation, building VerneMQ should be as
simple as:

```shell
$ cd $VERNEMQ
$ make rel
```    

### Starting VerneMQ

Once you've successfully built VerneMQ, you can start the server with the following
commands:

```shell
$ cd $VERNEMQ/_build/default/rel/vernemq
$ bin/vernemq start
```

If VerneMQ is running it is possible to check the status on
`http://localhost:8888/status` and it should look something like:


<img src="https://i.imgur.com/XajYjtb.png" width="75%">

Note that the `$VERNEMQ/_build/default/rel/vernemq` directory is a complete,
self-contained instance of VerneMQ and Erlang. It is strongly suggested that you
move this directory outside the source tree if you plan to run a production
instance.

[ci-workflow-badge]: https://github.com/gojekfarm/vernemq/workflows/ci/badge.svg
[test-workflow-badge]: https://github.com/gojekfarm/vernemq/actions/workflows/pr.yml/badge.svg

