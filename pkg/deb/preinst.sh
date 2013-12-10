#!/bin/sh
# Create cyanite user and group
USERNAME="cyanite"
GROUPNAME="cyanite"
getent group "$GROUPNAME" >/dev/null || groupadd -r "$GROUPNAME"
getent passwd "$USERNAME" >/dev/null || \
      useradd -r -g "$GROUPNAME" -d /usr/lib/cyanite -s /bin/false \
      -c "Cyanite metric storage daemon" "$USERNAME"
exit 0
