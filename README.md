# VerneMQ: A Distributed MQTT Broker

[![Build Status](https://travis-ci.org/vernemq/vernemq.svg?branch=master)](https://travis-ci.org/vernemq/vernemq)
[![Docker Pulls](https://img.shields.io/docker/pulls/erlio/docker-vernemq.svg)](https://hub.docker.com/r/erlio/docker-vernemq/)
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

VerneMQ implements the MQTT 3.1, 3.1.1 and 5.0 specifications. Currently the
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

## Commercial Support. Binary Packages. Documentation

Below you'll find a basic introduction to building and starting VerneMQ. For
more information about the binary package installation, configuration, and
administration of VerneMQ, please visit our documentation at [VerneMQ
Documentation](https://docs.vernemq.com) or checkout the product page
[VerneMQ](https://vernemq.com) if you require more information on the available
commercial [support options](https://vernemq.com/services.html).

## Quick Start

This section assumes that you have a copy of the VerneMQ source tree. To get
started, you need to first build VerneMQ.

### Building VerneMQ

Note: VerneMQ is compatible with Erlang/OTP 19, 20 and 21 and one of
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

If VerneMQ is running it is possible to check the status on
`http://localhost:8888/status` and it should look something like:


<img src="https://i.imgur.com/XajYjtb.png" width="75%">

Note that the `$VERNEMQ/_build/default/rel/vernemq` directory is a complete, 
self-contained instance of VerneMQ and Erlang. It is strongly suggested that you
move this directory outside the source tree if you plan to run a production 
instance.

### Important links

* #vernemq on freenode IRC
* [VerneMQ Documentation](https://docs.vernemq.com) 
* [Follow us on Twitter (@vernemq)!](https://twitter.com/vernemq)

## Experimental plugin: `vmq_swc` a more powerful metadata replication algorithm

As of VerneMQ 1.6 an alternative metadata replication algorithm is part of the
VerneMQ master and can be found in `apps/vmq_swc`. The plugin is in beta stadium
,but could be already very useful for larger clusters in scenarios with *a lot* 
of clients. The plugin is part of the official release and could can be enabled
using `metadata_plugin = vmq_swc` in the `vernemq.conf`. 

### Challenges with Plumtree

VerneMQ uses Plumtree for optimistic replication of the metadata, namely
subscriber data and retained messages. The Plumtree based metadata storage
relies on Merkle trees for its anti-entropy mechanism, that is a background
process that ensures the metadata gets synchronized even in the case an update
operation was missed. The initialization as well as the ongoing maintenance of
such Merkle trees are expensive, especially if *a lot* of items are managed by
the tree. Moreover, removing items from the tree isn't currently supported
(distributed deletes).  As a consequence one has to look out to not randomly
generate data (e.g. by random MQTT client ids or random topics used in retained
messages).

While some of those issues could be solved by improving the way VerneMQ uses Plumtree
it would most probably break backward compatibility and would have to wait until 2.0.
For this reason we decided to look at better alternatives, one that scales to millions
of items, where we could get rid of the Merkle trees, and get a better way to deal with
distributed deletes. One promising alternative is *Server Wide Clocks (SWC)*. SWC is a
novel distributed algorithm that provides multiple advantages. Namely a new efficient
and lightweight anti-entropy mechanism, reduced per-key causality information, and
*real* distributed deletes. More about the research behind SWC can be found in the
[scientific paper](https://haslab.uminho.pt/tome/files/global_logical_clocks.pdf).
