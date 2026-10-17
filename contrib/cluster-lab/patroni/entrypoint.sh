#!/bin/sh
# Renders /etc/patroni/patroni.yml from the environment, then runs Patroni.
# Each PostgreSQL node talks to the Consul server of its own slot, as in
# a deployment where every node carries one Consul server and one PostgreSQL.
set -eu

: "${PATRONI_NAME:?}" "${CONSUL_HOST:?}" "${PG_SUPERUSER_PASSWORD:?}" "${PG_REPLICATION_PASSWORD:?}"

# The file holds the superuser and replication passwords.
umask 077
cat >/etc/patroni/patroni.yml <<EOF
scope: forgejo-pg
namespace: /service/
name: ${PATRONI_NAME}
restapi:
  listen: 0.0.0.0:8008
  connect_address: ${PATRONI_NAME}:8008
consul:
  host: ${CONSUL_HOST}:8500
  register_service: true
bootstrap:
  dcs:
    ttl: 20
    loop_wait: 5
    retry_timeout: 5
    maximum_lag_on_failover: 1048576
    synchronous_mode: on
    synchronous_mode_strict: false
    failsafe_mode: true
    postgresql:
      use_pg_rewind: true
      parameters:
        wal_log_hints: "on"
        max_connections: 100
        shared_buffers: 64MB
        effective_cache_size: 128MB
        maintenance_work_mem: 16MB
  initdb:
    - encoding: UTF8
    - data-checksums
  post_init: /usr/local/bin/patroni-post-init
postgresql:
  listen: 0.0.0.0:5432
  connect_address: ${PATRONI_NAME}:5432
  data_dir: /var/lib/postgresql/patroni/data
  bin_dir: /usr/lib/postgresql/18/bin
  pgpass: /tmp/pgpass
  authentication:
    superuser: {username: postgres, password: "${PG_SUPERUSER_PASSWORD}"}
    replication: {username: replicator, password: "${PG_REPLICATION_PASSWORD}"}
    rewind: {username: postgres, password: "${PG_SUPERUSER_PASSWORD}"}
  pg_hba:
    - local all all trust
    - host replication replicator 0.0.0.0/0 scram-sha-256
    - host all all 0.0.0.0/0 scram-sha-256
  parameters:
    unix_socket_directories: /var/run/postgresql
EOF

exec patroni /etc/patroni/patroni.yml
