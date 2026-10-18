#!/usr/bin/env bash
# Writes the lab's secrets into .env (git-ignored) the first time, with
# random values, and adds any that is missing later. Never overwrites one:
# the volumes were created with them. compose.yaml reads them as ${VAR} and
# refuses to start without them. Read one with `make secret NAME=...`.
set -euo pipefail
LAB_DIR="$(cd "$(dirname "$0")/.." && pwd)"
env_file="$LAB_DIR/.env"
umask 077
touch "$env_file"
chmod 600 "$env_file"
# A last line edited by hand without a newline would swallow the first
# appended name.
if [ -s "$env_file" ] && [ -n "$(tail -c1 "$env_file")" ]; then
  echo >>"$env_file"
fi
add() { # <name> <value>
  if ! grep -q "^$1=" "$env_file"; then
    echo "$1=$2" >>"$env_file"
    echo "secrets: $1 generated in .env" >&2
  fi
}
for name in PG_SUPERUSER_PASSWORD PG_REPLICATION_PASSWORD FORGEJO_DB_PASSWORD FORGEJO_SECRET_KEY \
  LAB_ADMIN_PASSWORD VAULT_DEV_TOKEN WITNESS_OVERRIDE_SECRET E2E_ADMIN_PASSWORD E2E_USER_PASSWORD; do
  add "$name" "$(openssl rand -hex 24)"
done
# Nomad's ACL bootstrap token must be a UUID.
add NOMAD_TOKEN "$(openssl rand -hex 16 | sed -E 's/(.{8})(.{4})(.{4})(.{4})(.{12})/\1-\2-\3-\4-\5/')"
# Forgejo's LFS JWT secret, shared by the nodes (compose.ci.yaml): 32 bytes
# in unpadded base64url.
add LFS_JWT_SECRET "$(openssl rand 32 | base64 | tr '+/' '-_' | tr -d '=\n')"
