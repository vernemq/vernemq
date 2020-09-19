# VerneMQ: A Distributed MQTT Broker

[![Build Status](https://travis-ci.org/vernemq/vernemq.svg?branch=master)](https://travis-ci.org/vernemq/vernemq)
<a href="https://docs.vernemq.com">
		<img alt="Documentation" src="https://img.shields.io/badge/documentation-yes-brightgreen.svg" target="_blank" />
	</a>
	<a href="https://github.com/vernemq/vernemq/graphs/commit-activity">
		<img alt="Maintenance" src="https://img.shields.io/badge/maintained-yes-green.svg" target="_blank" />
	</a>
<a href="https://github.com/vernemq/vernemq/releases/latest">
<img alt="GitHub Release Date" src="https://img.shields.io/github/release-date/vernemq/vernemq"></a>
<a href="https://github.com/vernemq/vernemq/commits/master">
<img alt="GitHub last commit" src="https://img.shields.io/github/last-commit/vernemq/vernemq"></a>
<a href="https://twitter.com/vernemq">
		<img
			alt="Twitter: VerneMQ"
			src="https://img.shields.io/twitter/follow/vernemq.svg?style=social"
			target="_blank"
		/>
	</a>
[![Google group : VerneMQ Users](https://img.shields.io/badge/Google%20Group-VerneMQ%20Users-blue.svg)](https://groups.google.com/forum/#!forum/vernemq-users)
Old Docker Repo | New Docker Repo
------------ | -------------
[![Docker Pulls from Old Repo](https://img.shields.io/docker/pulls/erlio/docker-vernemq.svg)](https://hub.docker.com/r/erlio/docker-vernemq/)|[![Docker Pulls from New Repo](https://img.shields.io/docker/pulls/vernemq/vernemq.svg)](https://hub.docker.com/r/vernemq/vernemq/)

New: VerneMQ can now use Github Discussions! To join the discussion on features and roadmap, and be part of the <strong>VerneMQ Community Team</strong> on Github, send us your Github username for an invite! (on Twitter, Slack etc.)
- - - 

VerneMQ is known to be deployed and used in: :us: :canada: :brazil: :mexico: :de: :fr: :switzerland: :denmark: :netherlands: :belgium: :it: :es: :romania: :portugal: :ru: :lithuania: :czech_republic: :slovakia: :austria: :poland: :norway: :sweden: :india: :jp: :indonesia: :vietnam: :kr: :south_africa: :kenya: :serbia: :croatia: :greece: :uk: :ukraine: :australia: :new_zealand: :cn: :egypt: :finland: :hungary: :israel: :singapore: :lebanon: :philippines: :pakistan: :malaysia: :tr: :taiwan: :iran: :cloud:

---
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

## Commercial Support. Binary Packages. Documentation

Below you'll find a basic introduction to building and starting VerneMQ. For
more information about the binary package installation, configuration, and
administration of VerneMQ, please visit our documentation at [VerneMQ
Documentation](https://docs.vernemq.com) or checkout the product page
[VerneMQ](https://vernemq.com) if you require more information on the available
commercial [support options](https://vernemq.com/services.html).

## Community Release Schedule

Next major release: not yet scheduled.

Minor releases: At the end of March, July and November (every 4th month).

Bugfix releases: Usually a bugfix release is released between minor releases or
if there's an urgent bugfix pending.

Custom release cycles and releases are available for commercial users.

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

### Important links

* [VerneMQ Documentation](https://docs.vernemq.com)
* [![Google group : VerneMQ Users](https://img.shields.io/badge/Google%20Group-VerneMQ%20Users-blue.svg)](https://groups.google.com/forum/#!forum/vernemq-users)
* <a href="https://twitter.com/vernemq">
		<img
			alt="Twitter: VerneMQ"
			src="https://img.shields.io/twitter/follow/vernemq.svg?style=social"
			target="_blank"
		/>
	</a>
