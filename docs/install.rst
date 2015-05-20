.. _install:

Installation
============

VerneMQ builds on most operating systems where a recent Erlang version is installed. You can download and install VerneMQ as a package for your operating system, or you can build and run it from the sources yourself.
We currently provide binary packages for Debian Wheezy/Jessie, Ubuntu Precise/Trusty, and CentOS 6/7. These packages are hosted in our `PackageCloud Repository <https://packagecloud.io/erlio/vernemq>`_.

Install VerneMQ on Debians
--------------------------

To automatically add the package repository to your apt sources you could use the following script::

    curl https://packagecloud.io/install/repositories/erlio/vernemq/script.deb.sh | sudo bash

.. note::

    The script is essentially a shortcut for the following steps:

    First you need to install the PackageCloud gpg key that is used to sign repository metadata. You do that using ``apt-key``. ::

        curl https://packagecloud.io/gpg.key | apt-key add -

    You'll also need to install a package called apt-transport-https to make it possible for apt to fetch packages over https. ::

        apt-get install -y apt-transport-https

    The next thing you'll need to do is put a .list file in place. The easiest way to do that is to let the PackageCloud installation API generate one for you and modify it as you please. Here's an example curl request: ::

        HOSTNAME=`hostname -f` && curl "https://packagecloud.io/install/repositories/erlio/vernemq/config_file.list?os=ubuntu&dist=precise&name=${HOSTNAME}"

    You'll want to tailor that command to fit the system you're trying to install the repo on. We recommend you set the name to the fqdn of the target node (the value of hostname -f on linux), but you can set it to whatever you want.

Installing the package is as simple as::

    sudo apt-get install vernemq


Install VerneMQ on RedHats
--------------------------

To automatically add the package repository to your yum repos you could use the following script::

    curl https://packagecloud.io/install/repositories/erlio/vernemq/script.rpm.sh | sudo bash

.. note::

    The script is essentially a shortcut for the following steps:

    Before installing the .repo file, you'll probably need to install a package called ``pygpgme``, which allows yum to handle gpg signatures ::

        sudo yum install pygpgme

    The next thing you'll need to do is put a .repo file in place. The easiest way to do that is to let the PackageCloud installation API generate one for you and modify it as you please. Here's an example curl request: ::

        HOSTNAME=`hostname -f` && curl "https://packagecloud.io/install/repositories/erlio/vernemq/config_file.repo?os=el&dist=6&name=${HOSTNAME}"

    You'll want to tailor that command to fit the system you're trying to install the repo on. We recommend you set the name to the fqdn of the target node (the value of hostname -f on linux), but you can set it to whatever you want.

Installing the package is as simple as::

    sudo yum install vernemq


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
