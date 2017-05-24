# Cyanite

Cyanite is a daemon which provides services to store and retrieve timeseries data.
It aims to serve as a drop-in replacement for Graphite/Graphite-web.

# Getting Started

Before you begin, make sure you have the following installed:

  * [Java 1.7+](https://java.com/de/download/)
  * [Cassandra 3.5+](http://cassandra.apache.org/)

You can download the latest distribution of graphite from [GitHub releases](https://github.com/pyr/cyanite/releases)
and start it with:

```
java -jar <path-to-cyanite-jar>.jar --path <path-to-cyanite-config>.yaml
```

[See default configuration and basic configuration options.](https://github.com/pyr/cyanite/blob/master/doc/cyanite.yaml)

For advanced usage and information on possible Cyanite optimisations, refer to [configuration guide](http://cyanite.io/concepts.html).

# Getting help

You can get help by creating an issue or asking on IRC channel `#cyanite` on freenode.

For more information, refer to http://cyanite.io
