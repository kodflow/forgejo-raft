#!/bin/sh
# Patroni post_init: runs once, on the node that bootstraps the cluster,
# with a superuser connection string as $1. Creates the Forgejo role and
# database. Replicas get them through streaming replication.
set -eu

: "${FORGEJO_DB_PASSWORD:?}"

psql "$1" -v ON_ERROR_STOP=1 -v pw="${FORGEJO_DB_PASSWORD}" <<'SQL'
-- A cap well under max_connections (100): three nodes at 20 each fit.
CREATE ROLE forgejo LOGIN PASSWORD :'pw' CONNECTION LIMIT 60;
CREATE DATABASE forgejo OWNER forgejo ENCODING 'UTF8' TEMPLATE template0;
-- The durability check reads the standbys' positions (contrib/cluster/README.md).
GRANT pg_monitor TO forgejo;
SQL
