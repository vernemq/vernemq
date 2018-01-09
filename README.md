# VerneMQ: A Distributed MQTT Broker

[![Build Status](https://travis-ci.org/erlio/vernemq.svg?branch=master)](https://travis-ci.org/erlio/vernemq)
[![Slack Invite](https://slack-invite.vernemq.com/badge.svg)](https://slack-invite.vernemq.com)

[![VerneMQ Logo](https://i.imgur.com/bln3fK3.jpg)](https://vernemq.com)

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

VerneMQ implements the MQTT 3.1 and 3.1.1 specifications. Currently the
following features are implemented and delivered as part of VerneMQ:

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
* Memcached integration
* HTTP integration
* HTTP Webhooks
* PROXY Protocol v2
* Administration HTTP API (BETA)
* Real-time MQTT session tracing
* Full multitenancy

## Commercial Support. Binary Packages. Documentation

Below you'll find a basic introduction to building and starting VerneMQ. For
more information about the binary package installation, configuration, and
administration of VerneMQ, please visit our documentation at [VerneMQ
Documentation](https://vernemq.com/docs) or checkout the product page
[VerneMQ](https://vernemq.com) if you require more information on the available
commercial [support options](https://vernemq.com/services.html).

## Quick Start

This section assumes that you have a copy of the VerneMQ source tree. To get
started, you need to first build VerneMQ.

### Building VerneMQ

Note: VerneMQ is compatible with Erlang/OTP 18, 19 and 20 and one of
these versions is requred to be installed on your system.

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

Note that the `$VERNEMQ/_build/default/rel/vernemq` directory is a complete, 
self-contained instance of VerneMQ and Erlang. It is strongly suggested that you
move this directory outside the source tree if you plan to run a production 
instance.

### Important links

* #vernemq on freenode IRC
* [VerneMQ Documentation](http://vernemq.com/docs) 
* [Follow us on Twitter (@vernemq)!](https://twitter.com/vernemq)

