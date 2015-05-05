.. _plugins:

Plugins
=======

Many aspects of VerneMQ can be extended using plugins. The standard VerneMQ package comes with several *official* plugins:

.. toctree::
    :maxdepth: 1
    :glob:

    apps/vmq_*/docs/*

Check the installed plugins
---------------------------

.. code-block:: ini

    vmq-admin plugin show

The command above displays all the installed plugins together with the hooks they implement:

.. code-block:: ini

    +---------------+------------------------+
    |    Plugin     |         Hooks          |
    +---------------+------------------------+
    |    vmq_acl    |   auth_on_publish/6    |
    |               |  auth_on_subscribe/3   |
    |               |    change_config/1     |
    |  vmq_config   |    change_config/1     |
    |vmq_lvldb_store|msg_store_delete_async/1|
    |               |msg_store_delete_sync/1 |
    |               |    msg_store_fold/2    |
    |               |    msg_store_read/1    |
    |               |msg_store_write_async/2 |
    |               | msg_store_write_sync/2 |
    |  vmq_passwd   |   auth_on_register/5   |
    |               |    change_config/1     |
    |  vmq_systree  |    change_config/1     |
    +---------------+------------------------+

Enable a plugin
---------------

.. code-block:: ini

    vmq-admin plugin enable --name=vmq_snmp

The example above would enable the SNMP metric reporter plugin. Because the ``vmq_snmp`` plugin is already loaded (but not started) the above command succeeds. In case the plugin sits in an external directory you've also to provide the ``--path=<PathToPlugin>``.

Disable a plugin
----------------

.. code-block:: ini

    vmq-admin plugin disable --name=vmq_snmp
