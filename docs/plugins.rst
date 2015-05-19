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

    +-----------+-----------+-----------------+-----------------------------+
    |  Plugin   |   Type    |     Hook(s)     |            M:F/A            |
    +-----------+-----------+-----------------+-----------------------------+
    |vmq_systree|application|                 |                             |
    |vmq_passwd |application|auth_on_register |vmq_passwd:auth_on_register/5|
    |  vmq_acl  |application| auth_on_publish |  vmq_acl:auth_on_publish/6  |
    |           |           |auth_on_subscribe| vmq_acl:auth_on_subscribe/3 |
    +-----------+-----------+-----------------+-----------------------------+

Enable a plugin
---------------

.. code-block:: ini

    vmq-admin plugin enable --name=vmq_snmp

The example above would enable the SNMP metric reporter plugin. Because the ``vmq_snmp`` plugin is already loaded (but not started) the above command succeeds. In case the plugin sits in an external directory you've also to provide the ``--path=<PathToPlugin>``.

Disable a plugin
----------------

.. code-block:: ini

    vmq-admin plugin disable --name=vmq_snmp
