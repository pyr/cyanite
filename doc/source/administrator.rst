.. _Administrator Guide:

Administrator Guide
===================

This aims to be a simple guide for working with cyanite.


Administering Cassandra for Cyanite
-----------------------------------

Here are relevant topics when.

Choosing a Cassandra version
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Cyanite will work with Cassandra 2.0 and above, it has been tested
with the 2.1 releases extensively and thus is recommended.

Choosing a compaction strategy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``DateTieredCompactionStrategy``  is likely to be your best bet.

Choosing a read and write consistency level
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default Cyanite will read at consistency level ``ONE`` and
write at consistency level ``ANY``, thus favoring speed over
accuracy / consistency. You can specify alternative consistency
levels with the ``read-consistency`` and ``write-consistency`` sections
of the store configuration.

Cyanite out of band operations
------------------------------

The side-project: https://github.com/WrathOfChris/cyanite-utils provides
a few utilities to help with cyanite maintenance.
