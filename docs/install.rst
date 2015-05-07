.. _install:

Installation
============

VerneMQ builds on most operating systems where a recent Erlang version is installed. You can download and install VerneMQ as a package for your operating system, or you can build and run it from the sources yourself.

Install VerneMQ from sources
----------------------------

To install VerneMQ you will need to have a recent Erlang version installed. If you don't find Erlang already installed on your system, please follow the guide `Download Erlang OTP <https://www.erlang-solutions.com/downloads/download-erlang-otp>`_ from Erlang Solutions. If you prefer to build Erlang yourself, get the sources from `Erlang.org <http://www.erlang.org>`_.

VerneMQ depends on source code located in multiple `Git <http://git-scm.com>`_ repositories. Please make sure that ``git`` is installed on the target system.

Installing from GitHub
~~~~~~~~~~~~~~~~~~~~~~

The following instructions generate a complete, self-contained build of VerneMQ in ``$VERNEMQ/rel/vernemq`` where ``$VERNEMQ`` is the location of the unpacked or cloned source repository.

Clone the repository using Git and build a release::

    git clone git://github.com/erlio/vernemq.git
    cd vernemq
    make rel


At this point, you can start VerneMQ (with reasonable default settings) or you can configure it first according to your needs. Follow the :ref:`Starting VerneMQ <start_vernemq>` instructions or check the section on :ref:`Configuring VerneMQ <configure>`.


Microsoft Windows
~~~~~~~~~~~~~~~~~

VerneMQ is not currently supported on Microsoft Windows.
