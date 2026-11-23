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

Bind Mountpoints and Client IDs to Users
----------------------------------------

An optional binding file can restrict which MQTT Client IDs may be used by a
username on each mountpoint. Configure the file and enforcement mode in
``vernemq.conf``:

.. code-block:: ini

    vmq_passwd.client_id_file = /etc/vernemq/vmq.clientids
    vmq_passwd.client_id_binding = required

The binding file contains one ``mountpoint<TAB>username<TAB>client-id`` tuple
per line. The three fields must be separated by tab characters, not spaces.
Use the reserved mountpoint token ``""`` for VerneMQ's default empty
mountpoint. Empty lines and lines beginning with ``#`` are ignored. A tuple
may be repeated with different Client IDs to permit multiple clients.

.. code-block:: text

    # The <TAB> markers below represent literal tab characters
    ""<TAB>sensor<TAB>device-001
    ""<TAB>sensor<TAB>device-002
    tenant-a<TAB>operator<TAB>admin-console

The first two records apply to the default mountpoint. The third applies only
to ``tenant-a``. The literal value ``""`` is reserved and cannot be used to
represent a mountpoint actually named ``""``.

The available modes are:

``disabled``
    Do not check Client ID bindings. This is the default and preserves the
    previous authentication behavior.

``optional``
    A username present for the connection's mountpoint must use one of the
    Client IDs configured for that mountpoint. Username/mountpoint pairs not
    present in the binding file may use any Client ID that is not assigned to
    another username on that mountpoint.

``required``
    Every successfully authenticated user must also have an exact
    mountpoint/username/Client-ID tuple in the binding file.

The binding file is reloaded using the same interval as the password file.
Reloading is atomic: the complete file is parsed and validated before the new
set of bindings becomes active. Any malformed non-comment line invalidates the
entire file; bindings are never partially applied. Spaces cannot be used in
place of the two required tab separators.

Within one mountpoint, a Client ID may be assigned to only one username.
Repeating the exact same tuple is harmless, and the same Client ID may be
assigned independently on different mountpoints. Conflicting assignments
invalidate the complete file.

If binding is enabled and the binding file cannot be found, no bindings are
loaded. All ``vmq_passwd`` users are rejected in both ``optional`` and
``required`` modes until the file is successfully loaded again. This also
applies when the file exists but cannot be read or contains an invalid record.
An empty file, or a file containing only comments and empty lines, is invalid
when binding is enabled. To operate without bindings, explicitly configure
``vmq_passwd.client_id_binding = disabled``.

The binding file is security-sensitive even though it contains no passwords:
anyone who can modify it can authorize additional identity tuples. The
VerneMQ process should have read access, while only administrators or the
provisioning process should have write access. For a package installation
using the ``vernemq`` group, suitable permissions are:

.. code-block:: sh

    chown root:vernemq /etc/vernemq/vmq.clientids
    chmod 0640 /etc/vernemq/vmq.clientids

Use ``0600`` when the VerneMQ service account owns the file directly. The
parent directory must not be writable by untrusted users. Update the file by
writing a temporary file in the same directory, setting its ownership and
permissions, and atomically renaming it over the original. Install the same
binding file on every applicable cluster node; VerneMQ does not distribute it
automatically.

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
