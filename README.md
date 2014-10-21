cyanite: cassandra backed metric storage web service
====================================================

![cyanite](http://upload.wikimedia.org/wikipedia/commons/thumb/0/00/Kyanite_crystals.jpg/320px-Kyanite_crystals.jpg)

Cyanite is a metric storage daemon, exposing both
a carbon listener and a simple web service. Its aim is
to become a simple, scalable and drop-in replacement for
graphite's backend.

Graphite is a powerful graphing solution. It sports a somewhat aged but
very powerful web interface. Carbon is graphite's storage daemon,
written in python which writes out whisper or ceres file.

[![Build Status](https://travis-ci.org/pyr/cyanite.svg?branch=master)](https://travis-ci.org/pyr/cyanite)

## Compiling

Cyanite is a clojure application and thus can be built as a standalone JAR file.
Building cyanite needs a working [leiningen](http://leiningen.org) installation,
as well as a java JRE and JDK. Once the prerequisites are met run the following:

```
lein uberjar
```

The resulting artifact will be stored in `target/cyanite-0.1.0-standalone.jar`

## Runtime dependencies

You will need a running cassandra cluster, the simplest way to get up and running
is to follow the instructions as available here (using the `20x` branch):
http://wiki.apache.org/cassandra/DebianPackaging.

You will need a cassandra keyspace (rough equivalent of an SQL database) with the
following schema (also available in doc/schema.cql):

```
CREATE KEYSPACE metric WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

USE metric;

CREATE TABLE metric (
  period int,
  rollup int,
  tenant text,
  path text,
  time bigint,
  data list<double>,
  PRIMARY KEY ((tenant, period, rollup, path), time)
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='NONE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

```

You can create this keyspace by running the following command:

```
cqlsh < doc/schema.cql
```

## Configuring

Cyanite is configured from a `YAML` file. The default path for this
path is `/etc/cyanite.yaml` though a different path can be provided
on the command line.

```yaml
carbon:
  host: "127.0.0.1"
  port: 2003
  rollups:
    - period: 60480
      rollup: 10
    - period: 105120
      rollup: 600
http:
  host: "127.0.0.1"
  port: 8080
logging:
  level: info
  console: true
  files:
    - "/tmp/cyanite.log"
store:
  cluster: 'localhost'
  keyspace: 'metric'
```

You can also specify your rollups as [carbon-style](http://graphite.readthedocs.org/en/latest/config-carbon.html#storage-schemas-conf) retention description.

```yaml
carbon:
  rollups:
    - "10s:1d"
    - "10m:1y"
```

## Path Store
By default, cyanite stores all the graphite paths in memory. For small amounts of data or in non-HA setups,
this works pretty well. However, for metrics datasets with a large number of metrics or where HA is required,
elasticsearch can be used to store metrics instead, which results in faster response time and no in memory cache for
cyanite. This can be used both via the native Java elasticsearch interface, or the RESTful HTTP interface.

For REST API:
```yaml
index:
  use: "io.cyanite.es_path/es-rest"
  index: "my_paths" #defaults to "cyanite_paths"
  url: "http://myes.host.com:9200" #defaults to http://localhost:9200
```

For Native Java:
```yaml
index:
  use: "io.cyanite.es_path/es-native"
  index: "my_paths" #defaults to "cyanite_paths"
  host: "192.168.1.1" # defaults to localhost
  port: 9300 # defaults to 9300
  cluster_name: "" #REQUIRED! this is specific to your cluster and has no sensible default
```

## Running

```
 Switches                 Default  Desc
 --------                 -------  ----
 -h, --no-help, --help    false    Show help
 -f, --path                        Configuration file path
 -q, --no-quiet, --quiet  false    Suppress output
```

## Wiring up graphite-web to cyanite

To display cyanite graphs from grahite, you need to install graphite-web
from source using the master branch at https://github.com/graphite-project/graphite-web. You will also need the following module installed: https://github.com/brutasse/graphite-cyanite.
Stay tuned for a seamless installation procedure.

## Thanks

Thanks go out to @brutasse for his work on https://github.com/brutasse/graphite-cyanite and the addition of pluggable backends in graphite-web.

We're also indebted to the creator of graphite which we've been using for a while
and for the rest of the crew at exoscale.

## License

Copyright 2013 Pierre-Yves Ritschard <pyr@spootnik.org>

Permission to use, copy, modify, and distribute this software for any
purpose with or without fee is hereby granted, provided that the above
notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
