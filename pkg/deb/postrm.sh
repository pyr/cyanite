#!/bin/sh
set -e
if [ "$1" = "purge" ] ; then
	update-rc.d cyanite remove >/dev/null
fi
