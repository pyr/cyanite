#!/bin/sh
if [ -x "/etc/init.d/cyanite" ]; then
	invoke-rc.d cyanite stop || exit $?
fi
