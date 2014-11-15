.. _configure:

Configuration
=============

Every VerneMQ node has to be configured. Depending on the installation method and chosen platform the configuration file ``vernemq.conf`` resides at different locations. If VerneMQ was installed through a Linux package the default location for the configuration file is ``/etc/vernemq/vernemq.conf``.

File Format
-----------

- Everything you need to know about a single setting is on one line. 
- Lines are structured ``Key = Value``
- Any line starting with `#` is a comment, and will be ignored

Listeners
---------

Listeners specify on which IP address and port VerneMQ should accept new incoming connections. Depending on the chosen transport (TCP, SSL, WebSocket) different configuration parameters have to be provided. VerneMQ allows to write the listener configurations in a hierarchical manner, enabling very flexible setups. VerneMQ applies reasonable defaults on the top level, which can be of course overridden if needed.

.. code-block:: ini

    # defines the default nr of allowed concurrent 
    # connections per listener
    listener.max_connections = 10000

    # defines the nr. of acceptor processes waiting
    # to concurrently accept new connections
    listener.nr_of_acceptors = 10

    # used when clients of a particular listener should
    # be isolated from clients connected to another 
    # listener.
    listener.mountpoint = off

These are the only default parameters that are applied for all transports, and the only one that are of interest for plain TCP and WebSocket listeners.

These global defaults can be overridden for a specific transport protocol ``listener.tcp.CONFIG = VAL``, or even for a specific listener ``listener.tcp.LISTENER.CONFIG = VAL``. The placeholder ``LISTENER`` is freely chosen and is only used as a reference for further configuring this particular listener.


Sample Config
~~~~~~~~~~~~~

Listen on TCP port 1883 and for WebSocket Connections on port 8888:

.. code-block:: ini

    listener.tcp.default = 127.0.0.1:1883
    listener.ws.default = 127.0.0.1:8888

An additional listener can be added by using a different **name**. In the example above the **name** equals to ``default`` and can be used for further configuring this particular listener. The following example demonstrates how an additional listener is defined as well as how the maximum number of connections can be limited for this listener:

.. code-block:: ini

    listener.tcp.my_other = 127.0.0.1:18884
    listener.tcp.my_other.max_connections = 100

Sample SSL Config
~~~~~~~~~~~~~~~~~

Accepting SSL connections on port 8883:

.. code-block:: ini

    listener.ssl.cafile = /etc/ssl/cacerts.pem
    listener.ssl.certfile = /etc/ssl/cert.pem
    listener.ssl.keyfile = /etc/ssl/key.pem
    
    listener.ssl.default = 127.0.0.1:8883

If you want to use client certificates to authenticate your clients you have to set the following option:

.. code-block:: ini

    listener.ssl.require_certificate = on

If you use client certificates and want to use the certificates CN value as a username you can set:

.. code-block:: ini

    listener.ssl.use_identity_as_username = on
    
Both options ``require_certificate`` and ``use_identity_as_username`` default to ``off``.

The same configuration options can be used for securing WebSocket connections, just use ``wss`` as the protocol identifier e.g. ``listener.wss.require_certificate``.

General Broker Configurations
-----------------------------

.. _allow_anonymous:

Allow anonymous clients
~~~~~~~~~~~~~~~~~~~~~~~

Allow anonymous clients to connect to the broker:

.. code-block:: ini

    allow_anonymous = off

This option defaults to ``off``.

Password File
~~~~~~~~~~~~~

VerneMQ periodically checks the specified password file.

.. code-block:: ini

    password_file = /etc/vmq.passwd

This option allows you to specify a password file which stores username password pairs. Use the tool :ref:`vmq-passwd <vmq_passwd>` to add or delete new users.

ACL File
~~~~~~~~

VerneMQ periodically checks the specified ACL file.

.. code-block:: ini

    acl_file = /etc/vmq.acl

This option allows you to specify the access control list file that is used to control client access to the topics on the broker. Topic access is added with lines of the format:

.. code-block:: ini

    topic [read|write] <topic>

The access type is controlled using ``read`` or ``write``. If not provided then read an write access is granted for the ``topic``. The ``topic`` can use the MQTT subscription wildcards ``+`` or ``#``.

The first set of topics are applied to all anonymous clients (assuming ``allow_anonymous = on``, see :ref:`allow_anonymous`). User specific ACLs are added after a user line as follows:

.. code-block:: ini
    
    user <username>

The username referred to here is the same as in ``password_file``. It is not the client id.

It is also possible to define ACLs based on pattern substitution within the the topic. The form is the same as for the topic keyword, but using pattern as the keyword.

.. code-block:: ini

    pattern [read|write] <topic>

The patterns available for substitution are:

    *   ``%c`` to match the client id of the client
    *   ``%u`` to match the username of the client

The substitution pattern must be the only text for that level of hierarchy. Pattern ACLs apply to all users even if the **user** keyword has previously been given.

Example:

.. code-block:: ini

    pattern write sensor/%u/data


.. warning::
    
    VerneMQ currently doesn't cancel active subscriptions in case the ACL file revokes access for a topic.
    

Maximum Client Id Size
~~~~~~~~~~~~~~~~~~~~~~

Set the maximum size for client ids, MQTT v3.1 specifies a limit of 23 characters.

.. code-block:: ini

    max_client_id_size = 23

This option default to ``23``.

Retry Interval
~~~~~~~~~~~~~~

Set the time in seconds after a ``QoS=1 or QoS=2`` message has been sent that VerneMQ will wait before retrying when no response is received.

.. code-block:: ini

    retry_interval = 20

This option default to ``20`` seconds.

Persistent Client Expiration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This option allows persistent clients (those with ``clean_session`` set to ``false``) to be removed if they do not reconnect within a certain time frame.

.. note::

    This is a non-standard option. As far as the MQTT specification is concerned, persistent clients are persisted forever.

The expiration period should be an integer followed by one of ``h``, ``d``, ``w``, ``m``, ``y`` for hour, day, week, month, and year; or ``never``:

.. code-block:: ini

    persistent_client_expiration = 1w

This option defaults to ``never``.

$SYS Interval
~~~~~~~~~~~~~

This option sets the time in seconds between updates of the ``$SYS`` subscription hierarchy, which provides status information about the broker.

.. code-block:: ini

    sys_interval = 10

This option defaults to ``10`` seconds. If set to ``0`` VerneMQ won't publish any ``$SYS`` messages.

Inflight Messages
~~~~~~~~~~~~~~~~~

This option defines the maximum number of QoS 1 or 2 messages that can be in the process of being transmitted simultaneously.

.. code-block:: ini

    max_inflight_messages = 20

Defaults to ``20`` messages, use ``0`` for no limit.

Message Size Limit
~~~~~~~~~~~~~~~~~~

Limit the maximum publish payload size in bytes that VerneMQ allows. Messages that exceed this size won't be accepted.  

.. code-block:: ini

    message_size_limit = 0

Defaults to ``0``, which means that all valid messages are accepted. MQTT specification imposes a maximum payload size of 268435455 bytes.

Message Store
~~~~~~~~~~~~~

This option controls which message store backend VerneMQ uses. Per default the **Bitcask** backend is used, which saves every update to disk.

.. code-block:: ini

    msg_store.bitcask_backend = on

If the Bitcask backend is disabled (``off``) the messages are kept in memory.

In case you want to change the location of the message store you can set a path:

.. code-block:: ini

    msg_store.bitcask_backend.directory = /path/where/msg_store/should/go


Logging
-------

Console Logging
~~~~~~~~~~~~~~~

Where should VerneMQ emit the default console log messages (which are typically at ``info`` severity):

.. code-block:: ini

    log.console = off | file | console | both

VerneMQ defaults to log the console messages to a file, which can specified by:

.. code-block:: ini

    log.console.file = /path/to/log/file

This option defaults to the filename ``console.log``, whereas the path differs on the way VerneMQ is installed. 

The default console logging level ``info`` could be setting one of the following:

.. code-block:: ini

    log.console.level = debug | info | warning | error

Error Logging
~~~~~~~~~~~~~

VerneMQ defaults to log the error messages to a file, which can specified by:

.. code-block:: ini

    log.error.file = /path/to/log/file

This option defaults to the filename ``error.log``, whereas the path differs on the way VerneMQ is installed. 


SysLog
~~~~~~

VerneMQ supports logging to SysLog, enable it by setting:

.. code-block:: ini

    log.syslog = on


Bridges
-------

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
    #or
    bridge.ssl.br0.capath = /path/to/cacerts

    # if the remote broker requires client certificate authentication
    bridge.ssl.br0.certfile = /path/to/certfile.pem
    # and the keyfile
    bridge.ssl.br0.keyfile = /path/to/keyfile

    # disable the verification of the remote certificate (defaults to 'off')
    bridge.ssl.br0.insecure = off

    # set the used tls version (defaults to 'tlsv1.2')
    bridge.ssl.br0.tls_version = tlsv1.2


Monitoring
----------

Graphite
~~~~~~~~

VerneMQ can send metrics to a Graphite Server:

.. code-block:: ini

    # enable graphite_reporting (defaults to 'off')
    graphite_reporting = on

    graphite.host = carbon.hostedgraphite.com
    graphite.port = 2003
    graphite.api_key = YOUR-GRAPHITE-API-KEY

You can further tune the connection to the Graphite server:

.. code-block:: ini

    # set the connect timeout (defaults to 5000 ms)
    graphite.connect_timeout = 10000

    # set a graphite prefix (defaults to 'vernemq')
    graphite.prefix = myprefix

SNMP
~~~~

*TODO*

CollectD
~~~~~~~~

*TODO*


