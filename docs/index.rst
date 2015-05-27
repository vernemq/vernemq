.. VerneMQ documentation master file, created by
   sphinx-quickstart on Tue Oct 14 16:46:48 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.


VerneMQ: A Distributed MQTT Broker
==================================

Release v\ |release|. (:ref:`Installation <install>`)

VerneMQ is an :ref:`Apache2 licensed <apache2>` distributed `MQTT <http://www.mqtt.org>`_ broker, developed in `Erlang <http://www.erlang.org>`_.

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
* Reporting to Graphite and CollectD
* Extensible Plugin architecture
* Multiple Sessions per ClientId
* Session Balancing

.. note::

    Erlio GmbH, the main company sponsor behind the VerneMQ development provides commercial services around VerneMQ, namely M2M consulting, VerneMQ extension development, and service level agreements.

VerneMQ can be deployed on most platforms where a recent Erlang version (17 or higher) is available. Software packages for Redhats (and variants), Debians (and variants), will be provided. Please follow the :doc:`Installation <install>` instructions.


User Guide
----------

.. toctree::
    :maxdepth: 3

    install
    configure
    start
    clustering
    connect
    plugins
    tuning/overview
