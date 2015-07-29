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
  Type of input, available for now are "carbon" and "pickle"
*host*:
  Address to bind to. Valid for both "carbon" and "pickle"
*port*:
  Port to bind to. Valid for both "carbon" and "pickle"
  
.. sourcecode:: yaml
                
  input:
    - type: carbon
      port: 2003
    - type: pickle
      port: 2004

Index
~~~~~

The index determines where metric names will be stored.
Two types of indices are available now: "memory" and
"elasticsearch". If no index section is present,
An in-memory index will be assumed.

The memory index takes no options.
The elasticsearch index takes the following options:

*url*:
  The base URL for your ElasticSearch host, include
  the ES index in the URL. Defaults to
  http://localhost:9200/cyanite
*es-type*:
  The ES type to publish documents to, defaults to "path"
*mapping*:
  Provide a mapping for your type, defaults to:
  
  .. sourcecode:: yaml
  
     path:
       properties:
         path:
           type: string
           index: not_analyzed
         length:
           type: integer  
           index: not_analyzed
           store: false

.. sourcecode:: yaml
                
    index:
      type: memory

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

Cyanite exposes an API which is not fully
compatible with Graphite, to bridge cyanite
to Graphite or Grafana_, two options are available:

- Using alternative *storage finders* in graphite-web
- Using graphite-api

If you intend to use Grafana_, the recommended option
is to use graphite-api.

graphite-api configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~

You will need to install both `graphite-api` and
`graphite-cyanite` through pip. `graphite-api`
can then be configured by providing a valid YAML file
in `/etc/graphite-api.yaml`

.. sourcecode:: yaml

    search_index: /srv/graphite/index
    finders:
      - cyanite.CyaniteFinder
    cyanite:
      urls:
        - http://cyanite-host:port


`graphite-api` is fully documented at http://graphite-api.readthedocs.org/,
`graphite-cyanite` specific documentation can be found at
https://github.com/brutasse/graphite-cyanite.

graphite-web configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~

The only part which needs modifying once you have a working `graphite-web`
installation is to install `graphite-cyanite` and modify your
`local-settings.py` configuration file in Graphite:

.. sourcecode:: yaml

    STORAGE_FINDERS = ( 'cyanite.CyaniteFinder', )
    CYANITE_URLS = ( 'http://host:port', )
    
.. _Grafana: http://grafana.org

Administering Cassandra for Cyanite
-----------------------------------

Cassandra is a very versatile database - while still being ideally suited
for time-series type workloads. Here are a few pointers which might help when
operating a large metric cluster.

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
