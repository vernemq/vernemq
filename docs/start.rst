.. _start_vernemq:

Start VerneMQ
-------------

.. note::

    If you have build VerneMQ from sources, you can add the VerneMQ binary directory from the installation directory you've chosen to your ``PATH``.
    
    For example, if you compiled VerneMQ from source in the ``/home/vernemq`` directory, then you can add the binary directory (``/home/vernemq/rel/vernemq/bin``) to your ``PATH`` so that VerneMQ commands can be used in the same manner as with a packaged installation.

To start a VerneMQ node, use the ``vernemq start`` command in your Shell:

.. code-block:: sh

    vernemq start

A successful start will return no output. If there is a problem starting the node, an error message is printed to ``STDERR``.

To run VerneMQ with an attached interactive Erlang console:

.. code-block:: sh

    vernemq console

A VerneMQ node is typically started in console mode for debugging or troubleshooting purposes. Note that if you start VerneMQ in this manner, it is running as a foreground process that will exit when the console is closed.

You can close the console by issuing this command at the Erlang prompt:

.. code-block:: sh

    q().

Once your node has started, you can initially check that it is running with the ``vernemq ping`` command:

.. code-block:: sh

    vernemq ping

The command will respond with ``pong`` if the node is running or ``Node <NodeName> not responding to pings`` if it is not.

.. note:: 
    
    As you may have noticed, if you haven't adjusted your open files limit (ulimit -n), VerneMQ will warn you at startup. You're advised to increase the operating system default open files limit when running VerneMQ. You can read more about why in the :ref:`Open Files Limit <open_files_limit>` documentation. 
