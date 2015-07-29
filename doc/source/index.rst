.. Cyanite documentation master file, created by
   sphinx-quickstart on Wed Jul 22 11:34:53 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Cyanite
=======

.. image:: _static/cyanite.png
   :alt: cyanite logo
   :align: left
   :width: 100
              

Cyanite is a daemon which provides services to store and retrieve timeseries data.
It aims to be compatible with the Graphite_ eco-system.

Cyanite stores timeseries data in `Apache Cassandra`_ by default and focuses on
the following aspects:

Scalability
  By relying on `Apache Cassandra`_, Cyanite is able to provide highly available,
  elastic, and low-latency time-series storage. 

Compatibility
  The Graphite_ eco-system has become the de-facto standard for interacting
  with time-series data, either with ``graphite-web`` or with Grafana_.
  Cyanite will strive to remain as integrated as possible with this
  eco-system and to provide simple interaction modes.



.. raw:: html

    <p>
      <a class="reference external image-reference"
         href="https://github.com/pyr/cyanite">
        <img alt="GitHub project"
             src="https://img.shields.io/badge/GitHub-cyanite-green.svg"
             style="height: 18px !important; width: auto !important;">
      </a>

      <a class="reference external image-reference"
         href="https://travis-ci.org/pyr/cyanite">
        <img alt="Build Status"
             src="https://travis-ci.org/pyr/cyanite.svg?branch=master"
             style="height: 18px !important; width: auto !important;">
      </a>
    </p>

Cyanite is Open-Source software, released under an MIT license and is
available on github: https://github.com/pyr/cyanite. The latest
release is available at https://github.com/pyr/cyanite/releases/latest

.. warning::

   The updated Cyanite is a work in progress. Please stay tuned for the
   first release.

.. _Apache Cassandra: http://cassandra.apache.org
.. _Graphite: http://graphite.readthedocs.org
.. _Grafana: http://grafana.org

.. toctree::
   :maxdepth: 2

   quickstart
   concepts
   administrator
   api

