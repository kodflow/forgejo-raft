# PostgreSQL 18 + Patroni (Consul DCS) for the cluster lab.
# Patroni comes from PyPI, pinned, in a venv so it never touches the
# system Python of the image.
ARG POSTGRES_IMAGE=postgres:18@sha256:74935e72241653ca55e0414067e6d8763aceb8a810eb51b452253ec3dcfc4336
FROM ${POSTGRES_IMAGE}

ARG PATRONI_VERSION=4.1.5

RUN apt-get update -qq \
 && apt-get install -y -qq --no-install-recommends python3-venv curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && python3 -m venv /opt/patroni \
 && /opt/patroni/bin/pip install --no-cache-dir -q "patroni[consul,psycopg3]==${PATRONI_VERSION}" "psycopg[binary]" \
 && ln -s /opt/patroni/bin/patroni /usr/local/bin/patroni \
 && ln -s /opt/patroni/bin/patronictl /usr/local/bin/patronictl \
 && install -d -o postgres -g postgres -m 0700 /var/lib/postgresql/patroni \
 && install -d -o postgres -g postgres /var/run/postgresql /etc/patroni

COPY --chmod=0755 entrypoint.sh /usr/local/bin/patroni-entrypoint
COPY --chmod=0755 post_init.sh /usr/local/bin/patroni-post-init

USER postgres
ENTRYPOINT ["/usr/local/bin/patroni-entrypoint"]
