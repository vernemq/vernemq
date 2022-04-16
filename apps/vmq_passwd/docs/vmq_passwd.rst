.. _vmq_passwd:

vmq_passwd
==========

Simple Password based Client Authentication

VerneMQ periodically checks the specified password file.

.. code-block:: ini

    password_file = /etc/vmq.passwd

The check interval defaults to 10 seconds and can also be defined in the ``vernemq.conf``.

.. code-block:: ini

    password_reload_interval = 10

Setting the ``password_reload_interval = 0`` disables automatic reloading.

.. tip::
    
    Both configuration parameters can also be changed at runtime using the ``vmq-admin`` script.

Manage Password Files for VerneMQ
---------------------------------

``vmq-passwd`` is a tool for managing password files for the VerneMQ broker. Usernames must not contain ":", passwords are stored in similar format to `crypt(3) <http://man7.org/linux/man-pages/man3/crypt.3.html>`_.

How to use ``vmq-passwd``
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: sh
    
    vmq_passwd [-c | -D] passwordfile username

    vmq_passwd -U passwordfile

Options
~~~~~~~

``-c``
    
    Create a new password file. If the file already exists, it will be overwritten.
    
``-D``

    Delete the specified user from the password file.

``-U``

    This option can be used to upgrade/convert a password file with plain text passwords into one using hashed passwords. It will modify the specified file. It does not detect whether passwords are already hashed, so using it on a password file that already contains hashed passwords will generate new hashes based on the old hashes and render the password file unusable.


``passwordfile``

    The password file to modify.

``username``

    The username to add/update/delete.

Examples
~~~~~~~~

Add a user to a new password file:

.. code-block:: sh

    vmq-passwd -c /etc/vernemq/passwd henry

Delete a user from a password file

.. code-block:: sh

    vmq-passwd -D /etc/vernemq/passwd henry

Acknowledgements
~~~~~~~~~~~~~~~~

The original version of ``vmq-passwd`` was developed by Roger Light `<roger@atchoo.org>`_.

``vmq-passwd`` includes :

    *   software developed by the `OpenSSL Project <http://www.openssl.org/>`_ for use in the OpenSSL Toolkit. 

    *   cryptographic software written by Eric Young (`<eay@cryptsoft.com>`_)

    *   software written by Tim Hudson (`<tjh@cryptsoft.com>`_)

