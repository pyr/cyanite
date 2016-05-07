CONFIG_DIR        := /tmp/cyanite-data
CLUSTER_NAME      := cyanite_cluster
CASSANDRA_VERSION := binary:3.5

maybe_install_ccm:
	which ccm || test -s ~/.local/bin/ccm || pip install --user ccm

prepare_tmp_dir:
	rm -fr $(CONFIG_DIR) ;\
	mkdir -p $(CONFIG_DIR)

prepare_aliases:
	sudo ifconfig lo0 alias 127.0.0.2 up ;\
	sudo ifconfig lo0 alias 127.0.0.2 up

start_one_node_cluster: maybe_install_ccm prepare_tmp_dir
	ccm create $(CLUSTER_NAME) -n 1 -s -i 127.0.0. -b -v $(CASSANDRA_VERSION) --config-dir=$(CONFIG_DIR) ;\
  ccm node1 cqlsh < test/resources/schema.cql

start_three_node_cluster: maybe_install_ccm prepare_tmp_dir
	ccm create $(CLUSTER_NAME) -n 3 -s -i 127.0.0. -b -v $(CASSANDRA_VERSION) --config-dir=$(CONFIG_DIR) ;\
  ccm node1 cqlsh < test/resources/schema.cql

.PHONY: clean
stop_cluster:
	ps ax | grep java | grep org.apache.cassandra.service.CassandraDaemon | grep -v grep | awk '{print $$1}' | xargs kill -9

.PHONY: clean
clean:
	pip uninstall ccm
