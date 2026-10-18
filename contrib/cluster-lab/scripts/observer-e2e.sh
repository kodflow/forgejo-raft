#!/usr/bin/env bash
# The observer, end to end (contrib/cluster/observer.md), on the lab laid
# over by compose.observer.yaml. WRITTEN, NOT RUN YET: it starts every
# container of the lab plus an observer and a recovered node (about 4 GB);
# run it only with the lab owner's agreement.
#
#   1. members: checkpoints every 30 s, the export's sshd, the PostgreSQL
#      roles; the observer's keys, its pinned host keys, its recipient
#   2. a repository with a branch, an LFS object, an issue, and a user with
#      TOTP 2FA; the observer protects a checkpoint of them: the CLEAN one,
#      and the time from the push to its protection (the measured RPO)
#   3. the attacker: a valid force-push and a SQL DELETE of the issue; the
#      observer protects that checkpoint too (POISONED): it cannot tell
#   4. a PostgreSQL failover, a push on the new timeline, protected without
#      a gap in the archive
#   5. every member and every PostgreSQL is lost
#   6. a new node recovers the CLEAN checkpoint from its snapshot, mounted
#      read-only: rotation first, PostgreSQL replayed to the position with
#      the observer's configuration (recovery_target_inclusive = off, and the
#      same replay with on compared), refs, objects and files, verification,
#      then the new cluster with this node its only voter
#   7. the recovered forge serves the clean branch, the deleted issue, the
#      LFS object, and the 2FA login works
# Exits non-zero at the first failure. `make reset` starts over.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

dco() {
  docker compose --project-directory "$LAB_DIR" -f "$LAB_DIR/compose.yaml" -f "$LAB_DIR/compose.observer.yaml" \
    --profile observer --profile recover "$@"
}
# client <script>: git, git-lfs and python3 in a one-off container on the lab
# network, the scratch directory mounted on /work. Plain docker run: a
# compose run of observer would claim the observer's fixed address, which
# the running observer holds.
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
client() {
  docker run --rm -i -u 0 --network "${COMPOSE_PROJECT_NAME}_lab" -v "$WORK:/work" \
    --entrypoint /bin/sh forgejo-cluster-lab/forgejo:observer -c "$1"
}
# member <n> <cmd...>: a command as git on a running member.
member() { local n="$1"; shift; dco exec -T -u git "forgejo-$n" /usr/local/bin/gitea "$@"; }
# observer_list: the observer's checkpoints, read-only, as JSON.
observer_list() { dco exec -T -u git observer /usr/local/bin/gitea cluster observe list --json --dir /observer; }
# protected_after <unix seconds>: the newest PROTECTED checkpoint closed
# after that time, empty when there is none yet.
protected_after() {
  observer_list | python3 -c '
import json, sys, datetime
since = float(sys.argv[1])
for cp in json.load(sys.stdin) or []:
    t = datetime.datetime.fromisoformat(cp["time"].replace("Z", "+00:00")).timestamp()
    if cp["state"] == "PROTECTED" and not cp.get("problem") and t > since:
        print(cp["name"]); break' "$1"
}
wait_protected_after() {
  local since="$1" i name
  for ((i = 0; i < 180; i++)); do
    name="$(protected_after "$since" || true)"
    if [ -n "$name" ]; then
      echo "$name"
      return 0
    fi
    sleep 1
  done
  fail "no protected checkpoint after $since within 180 s"
}

USER_NAME=lab-admin
USER_PASS="$LAB_ADMIN_PASSWORD"
REPO="observed-$(date +%s)"
BASE="http://10.89.0.41:3000"

say "1. Members, observer, keys"
dco build observer
# The overlay's PostgreSQL archives: the nodes are recreated with it.
dco up -d --wait --no-build pg-1 pg-2 pg-3 haproxy
via_haproxy "DO \$\$ BEGIN CREATE ROLE forgejo_checkpoint NOLOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END \$\$;
  GRANT EXECUTE ON FUNCTION pg_create_restore_point(text) TO forgejo_checkpoint;
  GRANT EXECUTE ON FUNCTION pg_switch_wal() TO forgejo_checkpoint;
  GRANT EXECUTE ON FUNCTION pg_control_system() TO forgejo_checkpoint;
  GRANT forgejo_checkpoint TO forgejo;" forgejo >/dev/null
ok "checkpoint role granted to forgejo"
dco run --rm --no-deps -T -u 0 --entrypoint /bin/sh observer -c \
  'chown -R git:git /keys && [ -s /keys/id_ed25519 ] || su-exec git /usr/local/bin/gitea cluster observe keygen --name lab-observer --out /keys' >"$WORK/keygen.txt"
AUTH_LINE="$(grep '^restrict,' "$WORK/keygen.txt" | sed 's/<observer address>/10.89.0.80/; s|"forgejo cluster export|"/usr/local/bin/gitea cluster export|')"
RECIPIENT="$(grep '^OBSERVER_RECIPIENTS = ' "$WORK/keygen.txt" | sed 's/^OBSERVER_RECIPIENTS = //')"
[ -n "$AUTH_LINE" ] && [ -n "$RECIPIENT" ] || fail "observe keygen printed no key"
sed -i.bak '/^OBSERVER_RECIPIENTS=/d' "$LAB_DIR/.env" && rm -f "$LAB_DIR/.env.bak"
echo "OBSERVER_RECIPIENTS=$RECIPIENT" >>"$LAB_DIR/.env"
export OBSERVER_RECIPIENTS="$RECIPIENT"
dco up -d --no-build forgejo-1 forgejo-2 forgejo-3
: >"$WORK/known_hosts"
for n in 1 2 3; do
  wait_for 120 "forgejo-$n healthy" bash -c "[ \"\$(curl -s -o /dev/null -w '%{http_code}' $(forgejo_url $n)/api/healthz)\" = 200 ]"
  dco exec -T -u 0 "forgejo-$n" sh -c "printf '10.89.0.30:5000:*:replicator:%s\n' '$PG_REPLICATION_PASSWORD' >/data/pgpass && chown git:git /data/pgpass && chmod 0600 /data/pgpass"
  dco exec -d -u 0 "forgejo-$n" /usr/local/bin/sshd-export
  wait_for 20 "forgejo-$n export sshd up" dco exec -T "forgejo-$n" test -s /data/observer-sshd/sshd.pid
  echo "$AUTH_LINE" | dco exec -T -u 0 "forgejo-$n" sh -c 'cat >/data/observer-sshd/authorized_keys'
  echo "[10.89.0.4$n]:2222 $(dco exec -T "forgejo-$n" cat /data/observer-sshd/host_ed25519.pub | cut -d' ' -f1-2)" >>"$WORK/known_hosts"
done
dco run --rm --no-deps -T -u 0 -v "$WORK:/work" --entrypoint /bin/sh observer -c 'cp /work/known_hosts /keys/known_hosts && chown git:git /keys/known_hosts'
dco up -d --no-build observer
ok "observer started: no port published, keys pinned"

say "2. A repository, an LFS object, an issue, a user with 2FA; the CLEAN checkpoint"
member 1 admin user create --admin --username "$USER_NAME" --password "$USER_PASS" \
  --email "$USER_NAME@lab.invalid" --must-change-password=false >/dev/null 2>&1 || true
TOKEN="$(member 1 admin user generate-access-token --username "$USER_NAME" --token-name "obs-$REPO" --scopes all --raw | tail -n 1)"
curl -sf -u "$USER_NAME:$TOKEN" -X POST -H 'Content-Type: application/json' --data "{\"name\":\"$REPO\"}" \
  "$(forgejo_url 1)/api/v1/user/repos" >/dev/null
curl -sf -u "$USER_NAME:$TOKEN" -X POST -H 'Content-Type: application/json' --data '{"title":"the issue the attacker deletes"}' \
  "$(forgejo_url 1)/api/v1/repos/$USER_NAME/$REPO/issues" >/dev/null
# TOTP 2FA for a second account, through the web enrolment, the code
# computed here (RFC 6238, SHA-1, 30 s, 6 digits).
TFA_USER=lab-2fa
TFA_PASS="$(openssl rand -hex 12)"
member 1 admin user create --username "$TFA_USER" --password "$TFA_PASS" --email "$TFA_USER@lab.invalid" --must-change-password=false >/dev/null
cat >"$WORK/web.py" <<'PY'
import base64, hashlib, hmac, http.cookiejar, re, struct, sys, time, urllib.parse, urllib.request
def totp(secret):
    key = base64.b32decode(secret.upper() + "=" * (-len(secret) % 8))
    h = hmac.new(key, struct.pack(">Q", int(time.time()) // 30), hashlib.sha1).digest()
    o = h[-1] & 15
    return "%06d" % ((struct.unpack(">I", h[o:o + 4])[0] & 0x7fffffff) % 1000000)
base, user, pw, action = sys.argv[1:5]
jar = http.cookiejar.CookieJar()
web = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
# Forgejo forms carry no _csrf token any more (cross-origin protection by
# the Origin and Sec-Fetch-Site headers); one is sent only when present.
def csrf(path):
    page = web.open(base + path).read().decode()
    m = re.search(r'name="_csrf" value="([^"]+)"', page)
    return page, m.group(1) if m else None
def post(path, **form):
    form = {k: v for k, v in form.items() if v is not None}
    return web.open(base + path, urllib.parse.urlencode(form).encode())
_, tok = csrf("/user/login")
resp = post("/user/login", _csrf=tok, user_name=user, password=pw)
if action == "enroll":
    page, tok = csrf("/user/settings/security/two_factor/enroll")
    # The page shows the secret as text ("Or enter the secret: ...").
    secret = re.search(r"secret(?:=|:\s*)([A-Z2-7]{16,})", urllib.parse.unquote(page)).group(1)
    post("/user/settings/security/two_factor/enroll", _csrf=tok, passcode=totp(secret))
    open("/work/totp-secret", "w").write(secret)
else:
    secret = open("/work/totp-secret").read().strip()
    page, tok = csrf("/user/two_factor")
    post("/user/two_factor", _csrf=tok, passcode=totp(secret))
    page = web.open(base + "/user/settings").read().decode()
    assert user in page, "the 2FA login did not open a session"
print("ok")
PY
client "python3 /work/web.py $BASE $TFA_USER $TFA_PASS enroll" >/dev/null
via_haproxy "SELECT count(*) FROM two_factor" forgejo | grep -qx 1 || fail "2FA enrolment"
ok "$TFA_USER enrolled in TOTP 2FA"
head -c 2048 /dev/urandom >"$WORK/blob.bin"
client "cd /work && git init -q -b main repo && cd repo && git lfs install --local >/dev/null && git lfs track '*.bin' >/dev/null &&
  cp ../blob.bin . && echo clean >f.txt && git add . && git -c user.name=lab -c user.email=lab@lab.invalid commit -qm clean &&
  git push -q http://$USER_NAME:$TOKEN@10.89.0.41:3000/$USER_NAME/$REPO.git main && git rev-parse HEAD >/work/clean-sha"
CLEAN_SHA="$(cat "$WORK/clean-sha")"
PUSHED_AT="$(date +%s)"
CLEAN="$(wait_protected_after "$PUSHED_AT")"
RPO=$(($(date +%s) - PUSHED_AT))
ok "checkpoint $CLEAN protected ${RPO}s after the push (measured RPO; budget 60 s)"

say "3. The attacker: a force-push and a SQL DELETE, through the normal paths"
client "cd /work/repo && git checkout -q --orphan ransom && git rm -qrf . && echo pay >note.txt && git add note.txt &&
  git -c user.name=x -c user.email=x@x commit -qm ransom && git push -qf http://$USER_NAME:$TOKEN@10.89.0.41:3000/$USER_NAME/$REPO.git ransom:main"
via_haproxy "DELETE FROM issue WHERE repo_id = (SELECT id FROM repository WHERE lower_name = lower('$REPO'))" forgejo >/dev/null
POISON_AT="$(date +%s)"
POISONED="$(wait_protected_after "$POISON_AT")"
ok "the poisoned history is checkpoint $POISONED: protected like any other, the observer cannot tell"

say "4. A PostgreSQL failover, then a push on the new timeline"
OLD_TLI="$(observer_list | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["timeline"])')"
patronictl switchover --force >/dev/null
for ((i = 0; i < 60; i++)); do
  [ -n "$(haproxy_primary || true)" ] && break
  sleep 1
done
[ -n "$(haproxy_primary || true)" ] || fail "no primary behind HAProxy after the switchover"
# Forgejo's pooled connections die with the old primary: the first requests
# after the switch may fail, as in e2e.sh, so the push is retried.
client "cd /work/repo && echo after-failover >g.txt && git add g.txt && git -c user.name=x -c user.email=x@x commit -qm failover &&
  for i in \$(seq 60); do git push -qf http://$USER_NAME:$TOKEN@10.89.0.41:3000/$USER_NAME/$REPO.git HEAD:refs/heads/after-failover && exit 0; sleep 1; done; exit 1"
FAILOVER="$(wait_protected_after "$(date +%s)")"
NEW_TLI="$(observer_list | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["timeline"])')"
[ "$NEW_TLI" -gt "$OLD_TLI" ] || fail "the checkpoint after the failover is on timeline $NEW_TLI, not past $OLD_TLI"
ok "checkpoint $FAILOVER on timeline $NEW_TLI: the archive has no gap across the switch"

say "5. Every member and every PostgreSQL is lost"
dco stop -t 5 observer forgejo-1 forgejo-2 forgejo-3 pg-1 pg-2 pg-3
ok "the old cluster is gone; the observer's snapshots remain"

say "6. A new node recovers the CLEAN checkpoint"
NEW="$(openssl rand -hex 16)"
dco run --rm --no-deps -T -v "$LAB_DIR/pki/recert.sh:/lab/recert.sh:ro" \
  -v "${COMPOSE_PROJECT_NAME}_pki:/pki" -v "${COMPOSE_PROJECT_NAME}_forgejo-r:/nodes/node-r" --user 0 --entrypoint /bin/sh pki -c \
  "mkdir -p /nodes/node-r/tls && { [ -s /nodes/node-r/tls/node.key ] || openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out /nodes/node-r/tls/node.key; } && cp /pki/ca.pem /nodes/node-r/tls/ca.pem && /lab/recert.sh node-r $NEW"
dco run --rm --no-deps -T -u 0 --entrypoint /bin/sh observer -c \
  'rm -rf /keys/new && su-exec git /usr/local/bin/gitea cluster observe keygen --name lab-observer --out /keys/new' >"$WORK/keygen-new.txt"
NEW_RECIPIENT="$(grep '^OBSERVER_RECIPIENTS = ' "$WORK/keygen-new.txt" | sed 's/^OBSERVER_RECIPIENTS = //')"
dco up -d --no-build forgejo-r
dco exec -T -u 0 forgejo-r sh -c 'mkdir -p /data/pgsock /data/wal-archive && printf "local all all trust\n" >/data/lab-hba.conf && chown -R git:git /data/pgsock /data/wal-archive /data/lab-hba.conf'
PG_CTL="$(dco exec -T forgejo-r sh -c 'ls /usr/libexec/postgresql18/pg_ctl /usr/bin/pg_ctl 2>/dev/null | head -n 1' | tr -d '\r')"
[ -n "$PG_CTL" ] || fail "no pg_ctl in the observer image"
# rexec <args...>: the recovered node's command, with app.ini rendered from
# the environment plus the rotated secrets once they exist.
rexec() {
  dco exec -T -u 0 forgejo-r sh -c '
    [ -s /data/recover/rotated.env ] && { set -a; . /data/recover/rotated.env; set +a; }
    cd /etc/s6/gitea && . ./setup >/dev/null && exec su-exec git /usr/local/bin/gitea "$@"' -- "$@"
}
RECOVER=(cluster recover --from-observer "$CLEAN" --new-cluster "$NEW" --observer-dir "/snapshots/$CLEAN"
  --secrets-key /keys/secrets.key --new-observer-recipient "$NEW_RECIPIENT" --out /data/recover
  --pgdata /data/pg --pg-socket-dir /data/pgsock --pg-hba-file /data/lab-hba.conf --wal-archive-dir /data/wal-archive)
rexec cluster observe list --dir "/snapshots/$CLEAN" >"$WORK/list.txt" 2>&1 || true
# In its own snapshot a checkpoint reads VERIFIED: the hook takes the
# snapshot, then the observer writes the PROTECTED record (the live
# directory said PROTECTED in step 2). What matters here is that it restores.
grep -Eq "^$CLEAN .*(VERIFIED|PROTECTED).* yes\$" "$WORK/list.txt" || fail "$CLEAN does not restore from its snapshot: $(cat "$WORK/list.txt")"
ok "$CLEAN listed and restorable from its snapshot"

# Run 1 stops for the rotation: nothing restored yet.
if rexec "${RECOVER[@]}" >"$WORK/run1.txt" 2>&1; then fail "the recovery did not stop for the rotation"; fi
grep -q "rotated secrets are in" "$WORK/run1.txt" || fail "run 1: $(cat "$WORK/run1.txt")"
dco exec -T forgejo-r test ! -e /data/pg/PG_VERSION || fail "PostgreSQL was restored before the rotation"
dco exec -T -u 0 forgejo-r python3 -c '
import configparser
c = configparser.ConfigParser(); c.read("/data/recover/rotated.ini")
with open("/data/recover/rotated.env", "w") as f:
    for s in c.sections():
        for k, v in c.items(s):
            f.write("FORGEJO__%s__%s=%s\n" % (s, k.upper(), v))'
ok "rotation first: the secrets and the observer's new key are in the configuration"

# Run 2 prepares PostgreSQL and stops: no --pg-ctl yet. The prepared data
# directory is copied to replay it once more with inclusive on.
if rexec "${RECOVER[@]}" >"$WORK/run2.txt" 2>&1; then fail "the recovery did not stop for PostgreSQL"; fi
dco exec -T -u git forgejo-r sh -c 'grep -q "recovery_target_inclusive = off" /data/pg/postgresql.conf && ! grep -rq "archive_command" /data/pg/postgresql.auto.conf && cp -a /data/pg /data/pg-inclusive-on'
ok "PostgreSQL prepared with the observer's configuration, the source's dropped"

# The comparison: the same replay with recovery_target_inclusive = on, on
# its own port and socket (on the command line, over both configuration
# files), then stopped.
dco exec -T -u git forgejo-r sh -c "sed -i 's/recovery_target_inclusive = off/recovery_target_inclusive = on/; s/^port = .*/port = 5433/; s|unix_socket_directories = .*|unix_socket_directories = '/data/pgsock'|' /data/pg-inclusive-on/postgresql.conf &&
  $PG_CTL -D /data/pg-inclusive-on -o '-p 5433 -k /data/pgsock' -w -t 600 -l /data/pg-inclusive-on/log start >/dev/null || { tail -n 40 /data/pg-inclusive-on/log; exit 1; }"
ON_STATE="$(dco exec -T -u git forgejo-r psql -h /data/pgsock -p 5433 -U forgejo -d forgejo -Atc \
  "SELECT (SELECT count(*) FROM issue), (SELECT string_agg(commit_id, ',' ORDER BY name) FROM branch WHERE is_deleted = false)" | tr -d '\r')"
dco exec -T -u git forgejo-r "$PG_CTL" -D /data/pg-inclusive-on -w stop >/dev/null

# Run 3 starts PostgreSQL, checks its promotion, restores and verifies, and
# founds the new cluster.
rexec "${RECOVER[@]}" --pg-ctl "$PG_CTL" | tee "$WORK/run3.txt"
grep -q "verified: .* refs and SQL rows match" "$WORK/run3.txt" || fail "refs and SQL not verified"
grep -q "verified: .* 2FA secrets open" "$WORK/run3.txt" || fail "2FA secrets not verified"
grep -q "verified: .* LFS objects present" "$WORK/run3.txt" || fail "LFS objects not verified"
grep -q "every push, SQL write and stored file after them is lost" "$WORK/run3.txt" || fail "the loss is not stated"
OFF_STATE="$(dco exec -T -u git forgejo-r psql -h /data/pgsock -p 5432 -U forgejo -d forgejo -Atc \
  "SELECT (SELECT count(*) FROM issue), (SELECT string_agg(commit_id, ',' ORDER BY name) FROM branch WHERE is_deleted = false)" | tr -d '\r')"
[ "$OFF_STATE" = "$ON_STATE" ] || fail "inclusive off ($OFF_STATE) and on ($ON_STATE) disagree"
ok "inclusive off and on replay to the same rows: the position starts no record before it"

say "7. The recovered forge"
dco exec -d -u 0 forgejo-r sh -c 'set -a; . /data/recover/rotated.env; set +a; cd /etc/s6/gitea && . ./setup >/dev/null && exec su-exec git /usr/local/bin/gitea web'
wait_for 120 "forgejo-r healthy" bash -c '[ "$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:3009/api/healthz)" = 200 ]'
rexec cluster import >/dev/null
rexec cluster reconcile --apply >/dev/null || true
ok "the new cluster $NEW serves, the restored refs imported"
member_r() { curl -sf -u "$USER_NAME:$1" "http://127.0.0.1:3009/api/v1/$2"; }
NEW_TOKEN="$(rexec admin user generate-access-token --username "$USER_NAME" --token-name rec --scopes all --raw | tail -n 1)"
member_r "$TOKEN" "user" >/dev/null 2>&1 && fail "an access token of the old cluster still works"
ok "the old access tokens are revoked"
[ "$(git ls-remote "http://$USER_NAME:$NEW_TOKEN@127.0.0.1:3009/$USER_NAME/$REPO.git" refs/heads/main | cut -f1)" = "$CLEAN_SHA" ] ||
  fail "main is not the clean commit"
ok "main is the clean commit ${CLEAN_SHA:0:12}, not the force-push"
member_r "$NEW_TOKEN" "repos/$USER_NAME/$REPO/issues/1" >/dev/null || fail "the deleted issue is missing"
ok "the issue the attacker deleted is back"
client "cd /work && rm -rf check && git clone -q http://$USER_NAME:$NEW_TOKEN@10.89.0.49:3000/$USER_NAME/$REPO.git check && cd check && git lfs pull -q && cmp blob.bin ../blob.bin"
ok "the LFS object fetches, byte for byte"
client "python3 /work/web.py http://10.89.0.49:3000 $TFA_USER $TFA_PASS login" >/dev/null
ok "the 2FA login works with the restored SECRET_KEY"
echo "observer-e2e: recovered $CLEAN into $NEW; measured RPO ${RPO}s"
