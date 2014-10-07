#!/bin/sh
# Fakeroot and lein don't get along, so we set ownership after the fact.
set -e

chown -R root:root /usr/lib/cyanite
chown root:root /usr/bin/cyanite
chown cyanite:cyanite /var/log/cyanite
chown cyanite:cyanite /etc/cyanite.yaml
chown root:root /etc/init.d/cyanite

if command -v cqlsh > /dev/null 2>&1
then
	cqlsh -k metric -e "SELECT path from metric LIMIT 1" > /dev/null 2>&1 || (echo "Creating cassandra schema..." && cqlsh -f /var/lib/cyanite/schema.cql)
fi

if [ -x "/etc/init.d/cyanite" ]; then
	update-rc.d cyanite start 50 2 3 4 5 . stop 50 0 1 6 . >/dev/null
	invoke-rc.d cyanite start || exit $?
fi
