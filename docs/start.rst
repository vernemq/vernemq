.. _start_vernemq:

Starting VerneMQ
-------------

.. note::

    If you built VerneMQ from sources, you can add the ``/bin`` directory of your VerneMQ release to ``PATH``.

    For example, if you compiled VerneMQ in the ``/home/vernemq`` directory, then add the binary directory (``/home/vernemq/rel/vernemq/bin``) to your ``PATH``, so that VerneMQ commands can be used in the same manner as with a packaged installation.

To start a VerneMQ broker, use the ``vernemq start`` command in your Shell:

.. code-block:: sh

    vernemq start

A successful start will return no output. If there is a problem starting the broker, an error message is printed to ``STDERR``.

To run VerneMQ with an attached interactive Erlang console:

.. code-block:: sh

    vernemq console

A VerneMQ broker is typically started in console mode for debugging or troubleshooting purposes. Note that if you start VerneMQ in this manner, it is running as a foreground process that will exit when the console is closed.

You can close the console by issuing this command at the Erlang prompt:

.. code-block:: sh

    q().

Once your broker has started, you can initially check that it is running with the ``vernemq ping`` command:

.. code-block:: sh

    vernemq ping

The command will respond with ``pong`` if the broker is running or ``Node <NodeName> not responding to pings`` in case it's not.

.. warning::

    As you may have noticed, VerneMQ will warn you at startup when your system's open files limit (``ulimit -n``) is too low. You're advised to increase the OS default open files limit when running VerneMQ.

    Read more about why and how in the :ref:`Open Files Limit <open_files_limit>` documentation.
