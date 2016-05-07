# Running tests

In order to run integration tests, you have to have Cassandra running. You can
do it either manually or with `make start_one_node_cluster` script. It will
start a single node cluster with pre-loaded `cyanite_test` keyspace.

In order to shutdown the test cluster, run `make stop_cluster`.
