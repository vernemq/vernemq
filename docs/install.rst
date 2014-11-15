.. _install:

Installation
============

VerneMQ builds on most operating systems where a recent Erlang version is available. As soon as we are ready to deploy software packages, you'll find here the instructions how to download and install the package for your operating system. Until then you should build VerneMQ from sources.

Install VerneMQ from sources
----------------------------

To install VerneMQ you will need to have a recent Erlang version installed. If you don't have Erlang already installed, please follow the guide `Download Erlang OTP <https://www.erlang-solutions.com/downloads/download-erlang-otp>`_ from Erlang Solutions.

VerneMQ depends on source code located in multiple `Git <http://git-scm.com>`_ repositories. Please make sure that ``git`` is installed on the target system.

Installing from GitHub
~~~~~~~~~~~~~~~~~~~~~~

The following instructions generate a complete, self-contained build of VerneMQ in ``$VERNEMQ/rel/vernemq`` where ``$VERNEMQ`` is the location of the unpacked or cloned source.

Clone the repository using Git and build::

    git clone git://github.com/erlio/vernemq.git
    cd vernemq
    make rel


Once your build is ready you are ready to start. If you can live with the reasonable default settings follow the :ref:`Start VerneMQ <start_vernemq>` instructions or check the section on :ref:`Configuring VerneMQ <configure>`.


Microsoft Windows
~~~~~~~~~~~~~~~~~

VerneMQ is not currently supported on Microsoft Windows.
