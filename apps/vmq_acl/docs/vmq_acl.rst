.. _vmq_acl:

vmq_acl
=======

Simple ACL based Client Authorization

VerneMQ periodically checks the specified ACL file.

.. code-block:: ini

    acl_file = /etc/vmq.acl

The check interval defaults to 10 seconds and can also be defined in the ``vernemq.conf``.

.. code-block:: ini

    acl_reload_interval = 10

Setting the ``acl_reload_interval = 0`` disables automatic reloading.

.. tip::
    
    Both configuration parameters can also be changed at runtime using the ``vmq-admin`` script.

Managing the ACL entries
------------------------

Topic access is added with lines of the format:


.. code-block:: ini

    topic [read|write] <topic>

The access type is controlled using ``read`` or ``write``. If not provided then read an write access is granted for the ``topic``. The ``topic`` can use the MQTT subscription wildcards ``+`` or ``#``.

The first set of topics are applied to all anonymous clients (assuming ``allow_anonymous = on``). User specific ACLs are added after a user line as follows (this is the username not the client id):

.. code-block:: ini
    
    user <username>
    
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
