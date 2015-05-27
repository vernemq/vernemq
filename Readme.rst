VerneMQ: A Distributed MQTT Broker
==================================

VerneMQ is an Apache2 licensed distributed `MQTT <http://www.mqtt.org>`_ broker, developed in `Erlang <http://www.erlang.org>`_.

MQTT stands for MQ Telemetry Transport. It is an extremely simple and lightweight publish/subscribe messaging protocol, that was invented at IBM and Arcom (now Eurotech) to connect restricted devices in low bandwidth, high-latency or unreliable networks.

VerneMQ implements the MQTT 3.1 and 3.1.1 specifications, integration of MQTT-SN is planned. Currently the following features are implemented:

* QoS 0, QoS 1, QoS 2
* Basic Authentication and Authorization
* Bridge Support
* $SYS Tree for monitoring and reporting
* SSL Encryption
* Dynamic Topics
* Websockets Support
* Cluster Support
* SNMP Monitoring
* Logging (Console, Files, Syslog)
* Reporting to Graphite
* Extensible Plugin architecture
* Multiple Sessions per ClientId
* Session Balancing

For more information head over to the `VerneMQ Documentation <http://verne.mq/docs>`_ or to the product page `VerneMQ <http://verne.mq>`_.
