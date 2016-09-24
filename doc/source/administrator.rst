.. _Administrator Guide:

Administrator Guide
===================

This aims to be a simple guide for working with cyanite.

.. _Configuration Syntax:

Configuration Syntax
--------------------

Cyanite's configuration is broken up in different sections:

- engine
- api
- input
- index
- store
- logging

Most sections are optional but provide defaults
for a single host testing system.

Engine
~~~~~~

The engine specifies the behavior of Cyanite's core
which accepts metrics from inputs, aggregates in-memory
and defers to an index and a store when a time-window
elapses

The engine accepts the following options:

*rules*:
   Rules specifies which resolutions to apply to an incoming metric.
   Rules consist of a pattern or the string "default" and an associated
   list of resolutions.
   Rules are evaluated in a first-match order. Resolutions are stored as a
   string of the form: <precision>:<period>, you may use unit specifiers
   for seconds, minutes, hours, days, weeks and months and years.

.. sourcecode:: yaml

   engine:
     rules:
       "web.*\.cpu": [ "5s:1h", "30s:1d" ]
        default: [ "5s:1h" ]

API
~~~

The API specifies the behavior of the HTTP interface which is exposed.
The API accepts the following options:

*host*:
   Address to listen on, defaults to 127.0.0.1
*port*:
   Port to bind to, defaults to 8080
*disabled*:
   Disable HTTP service altogether, defaults to false.

.. sourcecode:: yaml

  api:
    port: 8080


Input
~~~~~

Inputs are methods for Cyanite to ingest metrics. A Cyanite installation
may have several inputs running, and thus accepts a list of input
configurations.

Each input configuration takes the following options:

*type*:
  Type of input, for now only "carbon"
*host*:
  Address to bind to.
*port*:
  Port to bind to.

.. sourcecode:: yaml

  input:
    - type: carbon
      port: 2003

Index
~~~~~

The index determines where metric names will be stored.
The only type of index available now is: "cassandra".
The cassandra index takes the following options:

*cluster*:
   A string or list of strings to provide cluster contact points.
*keyspace*:
   The keyspace to use.

.. sourcecode:: yaml

    index:
      type: cassandra
      keyspace: metric
      cluster: localhost

Metamonitoring
~~~~~~~~~~~~~~

To enable internal stats you must enable the internal reporter.


.. sourcecode:: yaml

    reporter:
      metrics:
        reporters:
          graphite:
            interval: 1
            opts:
              host: 127.0.0.1
              port: 2003
              prefix: internal


Store
~~~~~

The store is where metrics get persisted.
The only store available for now is the "cassandra"
one.

The following options are accepted:

*cluster*:
   A string or list of strings to provide cluster contact points.
*keyspace*:
   The keyspace to use.

.. sourcecode:: yaml

  store:
    cluster: 'localhost'
    keyspace: 'metric'

Logging
~~~~~~~

Specify where to log. Adheres to the configuration format
defined at https://github.com/pyr/unilog

.. sourcecode:: yaml

  logging:
    level: info
    console: true
    files:
      - "/var/log/cyanite/cyanite.log"


.. _Graphite Integration:

Integration with Graphite and Grafana
-------------------------------------

Cyanite exposes an API which Graphana can communicate with directly as if it were talking to graphite.

Administering Cassandra for Cyanite
-----------------------------------

Cassandra is a very versatile database - while still being ideally suited
for time-series type workloads. Here are a few pointers which might help when
operating a large metric cluster.

Choosing a Cassandra version
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Cyanite requires Cassandra 3.4 as it depends on SASI https://docs.datastax.com/en/cql/3.3/cql/cql_using/useSASIIndexConcept.html. It has been tested
with the 3.4 releases extensively and thus is recommended. 

Choosing a compaction strategy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``DateTieredCompactionStrategy``  is likely to be your best bet.

The following config causes most compaction activity to occur at 10m and 2h windows.\
If you want to allow 24h windows, simply raise max_sstable_age days to '1.0'.
Note that you must be using Apache Cassandra 2.1 in order to set fractional values for
max_sstable_age_days. If you are running an earlier version, then leave it at 1.

.. sourcecode:: json

    compaction = {'class': 'DateTieredCompactionStrategy',
    'min_threshold': '12', 'max_threshold': '32',
    'max_sstable_age_days': '0.083', 'base_time_seconds': '50' }

If you are willing to modify your Cassandra installation, ``TimeWindowCompactionStrategy`` gives great results
and fits the cyanite use case perfectly. To use it you will need to build the project yourself, as per instructions on
https://github.com/jeffjirsa/twcs. Once built, you can publish the JAR to the classpath of your Cassandra installation.
The following config can be used to take advantage of it:

.. sourcecode:: json

    compaction = {'unchecked_tombstone_compaction': 'false',
                  'tombstone_threshold': '0.2',
                  'class': 'com.jeffjirsa.cassandra.db.compaction.TimeWindowCompactionStrategy'}


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
