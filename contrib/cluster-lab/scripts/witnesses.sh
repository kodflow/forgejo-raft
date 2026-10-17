#!/usr/bin/env bash
# Starts the two Recover witnesses and points the Forgejo nodes at them
# (kitsunium ADR 0152 R9-08 to R9-11): their public keys and URLs, and the
# hash of the operator's boot override, go into .env, which compose reads;
# the nodes are recreated to take them.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

say "Start the witnesses"
dc up -d --wait --no-build witness-a witness-b
key_a="$(dc exec -T witness-a cat /witness/public-key | tr -d '\r\n')"
key_b="$(dc exec -T witness-b cat /witness/public-key | tr -d '\r\n')"
for k in "$key_a" "$key_b"; do
  echo "$k" | grep -Eq '^[0-9a-f]{64}$' || fail "unexpected witness key: $k"
done
ok "witness-a $key_a"
ok "witness-b $key_b"

override="$WITNESS_OVERRIDE_SECRET"
override_sha="$(printf '%s' "$override" | shasum -a 256 | cut -d' ' -f1)"
env_file="$LAB_DIR/.env"
# The copy holds every lab secret: private, like .env.
umask 077
touch "$env_file"
grep -vE '^(WITNESS_URLS|WITNESS_KEYS|WITNESS_OVERRIDE_SHA256)=' "$env_file" >"$env_file.tmp" || true
{
  cat "$env_file.tmp"
  echo "WITNESS_URLS=http://10.89.0.51:7482,http://10.89.0.52:7482"
  echo "WITNESS_KEYS=$key_a,$key_b"
  echo "WITNESS_OVERRIDE_SHA256=$override_sha"
} >"$env_file"
rm -f "$env_file.tmp"
ok "witness settings written to $env_file"

say "Recreate the Forgejo nodes with the boot fence"
dc up -d --wait --no-build forgejo-1 forgejo-2 forgejo-3
for n in 1 2 3; do
  dc logs --no-log-prefix "forgejo-$n" 2>&1 | grep "says cluster .* is clear" >/dev/null || fail "forgejo-$n did not run the boot check"
  ok "forgejo-$n started after the witnesses said clear"
done
