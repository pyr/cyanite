GRAFANA_DIR       := ./grafana/
STRESSER_DIR      := ./graphite-stresser/
CLUSTER_NAME      := cyanite_cluster
CASSANDRA_VERSION := binary:3.5

maybe_install_ccm:
	which ccm || test -s ~/.local/bin/ccm || pip install --user ccm

prepare_aliases:
	sudo ifconfig lo0 alias 127.0.0.2 up										;\
	sudo ifconfig lo0 alias 127.0.0.2 up

start_one_node_cluster: maybe_install_ccm
	ccm create $(CLUSTER_NAME) -v $(CASSANDRA_VERSION)			;\
	ccm populate -n 1 -i 127.0.0.														;\
	ccm start																								;\
	ccm node1 cqlsh < test/resources/schema.cql

reload_schema: maybe_install_ccm
	ccm node1 cqlsh < test/resources/schema.cql

.PHONY: clean
stop_cluster:
	ccm remove $(CLUSTER_NAME)

.PHONY: clean
clean:
	pip uninstall ccm


$(GRAFANA_DIR):
	cd $(GRAFANA_DIR)																				;\
	export GOPATH=`pwd`																			;\
	go get github.com/grafana/grafana												;\
	cd $GOPATH/src/github.com/grafana/grafana								;\
	go run build.go setup																		;\ # (only needed once to install godep)
	$GOPATH/bin/godep restore																;\ # (will pull down all golang lib dependencies in your current GOPATH)
	go run build.go build																		;\
	npm install																							;\
	npm install -g grunt-cli																;\
	grunt

grafana-server: $(GRAFANA_DIR)
	cd $(GRAFANA_DIR)																				;\
	cd src/github.com/grafana/grafana/											;\
	./bin/grafana-server

$(STRESSER_DIR):
	git clone git@github.com:feangulo/graphite-stresser.git ;\
	cd graphite-stresser																		;\
	./gradlew uberjar

STRESS_HOSTS			:= 100
STRESS_TIMERS			:= 20
STRESS_INTERVAL		:= 1
stress: $(STRESSER_DIR)
	java -jar $(STRESSER_DIR)/build/libs/graphite-stresser-0.1.jar localhost 2003 $(STRESS_HOSTS) $(STRESS_TIMERS) $(STRESS_INTERVAL) true

.PHONY: dev
dev:
	lein run --path ./dev/cyanite.yaml
