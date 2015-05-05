.. _clustering:

Clustering
==========

VerneMQ can be easily clustered, enabling that clients can connect to any cluster node and receive messages published from a client connected to a different node. However, the MQTT specification comes up with certain guarantees that are hard to fulfill in a distributed environment, especially when network partitions occur. We'll discuss the way VerneMQ deals with network partitions in [Dealing with Network Partitions](dealing-with-network-partitions).

Joining a Cluster
-----------------

.. code-block:: ini

    vmq-admin node join discovery-node=<OtherClusterNode>

Leaving a Cluster
-----------------

.. code-block:: ini

    vmq-admin node leave node=<NodeThatShouldGo>

.. danger::

    Currently the persisted QoS 1 & QoS 2 messages aren't replicated to the other nodes by the default message store backend. Currently you will **loose** the offline messages stored on the leaving node.

Getting Cluster Status Information
----------------------------------

.. code-block:: ini

    vmq-admin node status
