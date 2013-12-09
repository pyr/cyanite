#!/bin/sh
# Fakeroot and lein don't get along, so we set ownership after the fact.
chown -R root:root /usr/lib/cyanite
chown root:root /usr/bin/cyanite
chown cyanite:cyanite /var/log/cyanite
chown cyanite:cyanite /etc/cyanite.yaml
chown root:root /etc/init.d/cyanite
