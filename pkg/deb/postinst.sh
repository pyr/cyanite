#!/bin/sh
# Fakeroot and lein don't get along, so we set ownership after the fact.
set -e

chown -R root:root /usr/lib/cyanite
chown root:root /usr/bin/cyanite
chown cyanite:cyanite /var/log/cyanite
chown cyanite:cyanite /etc/cyanite.yaml
chown root:root /etc/init.d/cyanite

readonly KEYSPACE=metric

if command -v cqlsh > /dev/null 2>&1; then
	if cqlsh -e "QUIT" > /dev/null 2>&1; then
		if cqlsh -k $KEYSPACE -e "SELECT path from metric LIMIT 1" > /dev/null 2>&1; then
			echo "Cassandra keyspace '$KEYSPACE' already exists."
		else
			echo "Creating Cassandra schema in the keyspace '$KEYSPACE'..."
			cqlsh -f /var/lib/cyanite/schema.cql
		fi
	else
		echo "Cannot connect to Cassandra. Skipping schema creation."
	fi
else
	echo "Cassandra CQL Shell not found."
fi

if [ -x "/etc/init.d/cyanite" ]; then
	update-rc.d cyanite start 50 2 3 4 5 . stop 50 0 1 6 . >/dev/null
	invoke-rc.d cyanite start || exit $?
fi
