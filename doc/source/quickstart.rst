Quickstart Guide
================

Getting up and running with Cyanite involves two things which
we'll cover in this quick walk-through:

- Installing, configuring, and running `Apache Cassandra`_.
- Installing, configuring, and running Cyanite itself.

Obtaining Cyanite
-----------------

Cyanite is released in both source and binary.

Binary releases
~~~~~~~~~~~~~~~

Cyanite currently has no binary releases, as it's under active development.
We are getting clos to first stable version.

Binary release are the simplest way to get started and are hosted on github:
https://github.com/pyr/cyanite/releases/latest.

Each release contains:

- A source code archive
- A standard build (*cyanite-VERSION-standalone.jar*)
- A debian package

Requirements
------------

Runtime requirements
~~~~~~~~~~~~~~~~~~~~

Runtime requirements for Cyanite are kept to a minimum

- Java 8 Runtime (Sun JDK recommended)
- Apache Cassandra 3.4 or later

Build requirements
~~~~~~~~~~~~~~~~~~

If you wish to build Cyanite you will additionally need the
`leiningen`_ build tool to produce working artifacts. Once
leiningen_ is installed, you can just run ``lein uberjar`` to
produce a working Java archive.

Minimal configuration
----------------------

Cyanite is configured with a single configuration file, formatted in YAML.
See :ref:`Configuration Syntax` for more details

.. sourcecode:: yaml

    logging:
      level: info
      console: true
      files:
        - "/var/log/cyanite/cyanite.log"
    input:
      - type: "carbon"
    store:
      cluster: "127.0.0.1"
    index:
      type: "cassandra"
      keyspace: "metric"
      cluster: "127.0.0.1"
    api:
      port: 8080
    engine:
      rules:
        default:
          - "5s:1h"

Running Cyanite
---------------

Command-line arguments
~~~~~~~~~~~~~~~~~~~~~~

Cyanite accepts the following arguments::

    Switches                 Default  Desc
    --------                 -------  ----
    -h, --no-help, --help    false    Show help
    -f, --path                        Configuration file path
    -q, --no-quiet, --quiet  false    Suppress output

.. _leiningen: https://leiningen.org
.. _Apache Cassandra: http://cassandra.apache.org

Default Schema
--------------

The following schema has to be loaded in Cassandra in order to store data:

.. literalinclude:: ./../schema.cql
