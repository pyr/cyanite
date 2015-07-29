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

Binary release are the simplest way to get started and are hosted on github:
https://github.com/exoscale/cyanite/releases/latest.

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
- Apache Cassandra 2.1 or later

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
    inputs:
      - type: "carbon"
      - type: "pickle"
    store:
      type: "cassandra-v2"
      cluster: "127.0.0.1"
    index:
      type: "memory"
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
    
    


          
