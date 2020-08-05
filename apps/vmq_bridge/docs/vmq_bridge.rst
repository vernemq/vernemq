.. _vmq_bridge:

vmq_bridge
==========

Bridges are a non-standard way, although kind of a de-facto standard among MQTT broker implementations, to connect a single broker to a different broker. This allows for example that a topic tree of a remote broker gets part of the topic tree on the local broker. VerneMQ supports plain TCP connections as well as SSL connections.

Sample TCP Bridge
~~~~~~~~~~~~~~~~~

Setup a bridge to a remote broker:

.. code-block:: ini

    bridge.tcp.br0 = 192.168.1.12:1883

Different connection parameters can be set:

.. code-block:: ini

    # use a clean session (defaults to 'off')
    bridge.tcp.br0.cleansession = off | on

    # set the client id (defaults to 'auto', which generates one)
    bridge.tcp.br0.client_id = auto | my_bridge_client_id

    # set keepalive interval (defaults to 60 seconds)
    bridge.tcp.br0.keepalive_interval = 60

    # set the username and password for the bridge connection
    bridge.tcp.br0.username = my_bridge_user
    bridge.tcp.br0.password = my_bridge_pwd

    # set the restart timeout (defaults to 10 seconds)
    bridge.tcp.br0.restart_timeout = 10

    # VerneMQ indicates other brokers that the connection
    # is established by a bridge instead of a normal client.
    # This can be turned off if needed:
    bridge.tcp.br0.try_private = off

Define the topics the bridge should incorporate in its local topic tree, or the topics it should export to the remote broker. We share a similar configuration syntax as the Mosquitto broker: 

.. code-block:: ini

    topic [[[ out | in | both ] qos-level] local-prefix remote-prefix]

``topic`` defines a topic pattern that is shared between the two brokers. Any topics matching the pattern (which may include wildcards) are shared. The second parameter defines the direction that the messages will be shared in, so it is possible to import messages from a remote broker using ``in``, export messages to a remote broker using ``out`` or share messages in ``both`` directions. If this parameter is not defined, VerneMQ defaults to ``out``.The QoS level defines the publish/subscribe QoS level used for this topic and defaults to ``0``. *(Source: mosquitto.conf)*

The ``local-prefix`` and ``remote-prefix`` can be used to prefix incoming or outgoing publish messages.

.. warning::

    Currently the ``#`` wildcard is treated as a comment from the configuration parser, please use ``*`` instead. 

A simple example:

.. code-block:: ini

    # share messages in both directions and use QoS 1  
    bridge.tcp.br0.topic.1 = /demo/+ both 1
    
    # import the $SYS tree of the remote broker and 
    # prefix it with the string 'remote' 
    bridge.tcp.br0.topci.2 = $SYS/* in remote


Sample SSL Bridge
~~~~~~~~~~~~~~~~~

SSL bridges support the same configuration parameters as TCP bridges, but need further instructions for handling the SSL specifics:

.. code-block:: ini

    # define the CA certificate file or the path to the
    # installed CA certificates
    bridge.ssl.br0.cafile = cafile.crt

    # if the remote broker requires client certificate authentication
    bridge.ssl.br0.certfile = /path/to/certfile.pem
    # and the keyfile
    bridge.ssl.br0.keyfile = /path/to/keyfile

    # disable the verification of the remote certificate (defaults to 'off')
    bridge.ssl.br0.insecure = off

    # set the used tls version (defaults to 'tlsv1.2')
    bridge.ssl.br0.tls_version = tlsv1.2
