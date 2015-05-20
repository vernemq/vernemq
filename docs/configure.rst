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

Allow anonymous clients
-----------------------

Allow anonymous clients to connect to the broker:

.. code-block:: ini

    allow_anonymous = off

This option defaults to ``off``.

Authentication & Authorization
------------------------------

All the authentication and authorization handling is implemented by plugins. VerneMQ bundles the file based authentication plugin :ref:`vmq_passwd <vmq_passwd>` and a file based authorization plugin called :ref:`vmq_acl <vmq_acl>`.


Maximum Client Id Size
----------------------

Set the maximum size for client ids, MQTT v3.1 specifies a limit of 23 characters.

.. code-block:: ini

    max_client_id_size = 23

This option default to ``23``.

Retry Interval
--------------

Set the time in seconds after a ``QoS=1 or QoS=2`` message has been sent that VerneMQ will wait before retrying when no response is received.

.. code-block:: ini

    retry_interval = 20

This option default to ``20`` seconds.

Persistent Client Expiration
----------------------------

This option allows persistent clients (those with ``clean_session`` set to ``false``) to be removed if they do not reconnect within a certain time frame.

.. warning::

    This is a non-standard option. As far as the MQTT specification is concerned, persistent clients are persisted forever.

The expiration period should be an integer followed by one of ``h``, ``d``, ``w``, ``m``, ``y`` for hour, day, week, month, and year; or ``never``:

.. code-block:: ini

    persistent_client_expiration = 1w

This option defaults to ``never``.

Inflight Messages
-----------------

This option defines the maximum number of QoS 1 or 2 messages that can be in the process of being transmitted simultaneously.

.. code-block:: ini

    max_inflight_messages = 20

Defaults to ``20`` messages, use ``0`` for no limit.

Message Size Limit
------------------

Limit the maximum publish payload size in bytes that VerneMQ allows. Messages that exceed this size won't be accepted.  

.. code-block:: ini

    message_size_limit = 0

Defaults to ``0``, which means that all valid messages are accepted. MQTT specification imposes a maximum payload size of 268435455 bytes.

Console Logging
---------------

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
-------------

VerneMQ defaults to log the error messages to a file, which can specified by:

.. code-block:: ini

    log.error.file = /path/to/log/file

This option defaults to the filename ``error.log``, whereas the path differs on the way VerneMQ is installed. 


SysLog
------

VerneMQ supports logging to SysLog, enable it by setting:

.. code-block:: ini

    log.syslog = on
