# Running tests

In order to run integration tests, you have to have Cassandra running. You can
do it either manually or with `make start_one_node_cluster` script. It will
start a single node cluster with pre-loaded `cyanite_test` keyspace.

In order to shutdown the test cluster, run `make stop_cluster`.

# Making sure grafana integration works

You can start a local grafana server along with the cyanite and a single
node cassandra cluster from our `Makefile`. For that, you should run

```
make start_one_node_cluster
make grafana-server
```

Ideally, it will download, compile and set up everything you may need.
Usually grafana wants `go` and `nodejs` to be pre-installed. On Mac Os X,
you can install them via

```
brew install go
brew install godep
brew install nvm
# Set-up nvm
nvm install v6.1.0
```

# Running stress-tests

In order to run `graphite-stresser` against cyanite, you can run

```
make stress
```

All additional configuration options are available via (graphite-stresser)[https://github.com/feangulo/graphite-stresser]
itself.
