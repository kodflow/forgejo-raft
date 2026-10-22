# Active/active Forgejo: operator reference

This is the operator's reference for running Forgejo on three nodes that all
accept writes. The tutorial, with the architecture, a walk through a push and
the failure scenarios, is the repository's README (`.github/README.md`).
Deployment examples live in `contrib/cluster/deploy`, and a disposable Docker
lab in `contrib/cluster-lab`.

Every ref write goes through a consensus log (the `kitsunium/sdk` engine). A
write is committed once a majority of the nodes holds it on disk, together
with its git objects. What the consensus replicates is the ref table of every
repository; each node's git repositories are a projection of it. The SQL
database is shared (PostgreSQL, for example under Patroni), and what pushes
derive in it is written by a single consumer, on the leader.

## Requirements

- Three nodes, each with a management address the other two can reach.
- A shared PostgreSQL database, declared in the usual `[database]` section,
  durable across its own failovers (see "PostgreSQL durability", REQUIRED).
- A cluster certificate authority, and for each node a certificate carrying a
  single URI SAN `kitsunium-consensus://<CLUSTER_ID>/slot/<SLOT>`, with the
  `serverAuth` and `clientAuth` usages. The key stays on the node.
- `[security] DISABLE_GIT_HOOKS = true`. Start-up fails without it, since a
  user hook would run inside the replication path.
- git 2.48 or later. Start-up fails with an older git: the cluster learns
  HEAD only from the reference-transaction hook, which git before 2.46
  bypasses for `git init` and `git symbolic-ref`, and git up to 2.47 reports
  a HEAD line for a push to the default branch, which the cluster refuses.
  The standalone release binaries run with the distribution's git (2.39 on
  Debian 12, 2.43 on Ubuntu 24.04): upgrade it first.
- `/run/kitsunium-consensus`, a directory on the machine's tmpfs, emptied at
  each boot, writable by the user Forgejo runs as, and kept across restarts
  of Forgejo: the consensus node writes its boot-scoped `STORAGE_FAILED`
  marker there, the second line of defence when the marker in `DATA_DIR`
  cannot be written. The systemd unit (`RuntimeDirectory=` with
  `RuntimeDirectoryPreserve=yes`), the Nomad job (a host volume), the
  Kubernetes StatefulSet (a `hostPath`) and the Compose file (a bind mount)
  provide it. A consensus node that stops itself ends Forgejo with its exit
  code (65, 69, 73, 74 or 76): do not restart Forgejo on those codes
  (`RestartPreventExitStatus=`), an operator clears the stop. After a
  `STORAGE_FAILED`, reboot the MACHINE, never only the service: a restart in
  the same boot can read back pages a failed `fsync` left behind.

## PostgreSQL durability (REQUIRED)

The consumer writes what pushes derive in SQL, then trims the replicated
event queue behind it. A commit PostgreSQL acknowledged and then lost in a
failover would lose those events for good. The cluster therefore REQUIRES:

- `synchronous_commit` `on` or `remote_apply`, and `fsync` `on`;
- a synchronous standby: under Patroni, `synchronous_mode: on` AND
  `synchronous_mode_strict: true`. Without strict mode, Patroni empties
  `synchronous_standby_names` when its standby goes away and acknowledges
  commits no standby holds; a failover then rolls them back;
- Forgejo's role a member of `pg_monitor` (`GRANT pg_monitor TO forgejo`), so
  it can read the standbys' positions in `pg_stat_replication`.

The cluster checks this at start and before every trim of the event queue:
it reads the cursors and the parked events, then PostgreSQL's WAL position,
and trims only once every synchronous standby flushed past it. While the
check fails, nothing is trimmed (the queue grows, pushes are refused when it
is full, nothing is lost), `/api/healthz` warns under
`cluster:sql_durability` and the administration page says why. A strict
Patroni without a standby blocks writes instead: that is the intended
trade. SQLite, a local file, is durable by itself; a single PostgreSQL
without any standby declares `[cluster] SQL_STANDALONE = true`.

If the consumer reads a cursor behind one it saw (a failover or restore that
lost commits), it logs it, counts
`forgejo_cluster_consumer_cursor_regressions_total` and replays the events
past it from the log: they are still there.

## Parked events

An event whose post-receive work keeps failing (5 attempts) or whose
repository is not projected within 10 minutes is PARKED, never dropped: a
row of `cluster_parked_event` holding the event commits with the cursor that
moves past it. The later events of the same repository are parked behind it,
so its work keeps its order. The event queue is not trimmed at or past a
parked event. The leader retries the parked events every 30 seconds, and
`forgejo cluster reconcile --apply` retries them at once; a retry's work and
the removal of the row commit in one transaction, so each event is applied
exactly once. As soon as one is parked, it is logged as an error, listed
with its error on the administration page and warned under
`cluster:parked_events` in `/api/healthz`. When the event queue reaches half
of its bound (64 MiB), `cluster:event_queue` warns and the administration
says so, before pushes are refused.

A parked event that can never succeed (its repository or pusher ID was
reissued) is given up by an operator, never silently:

```sh
forgejo cluster parked list
forgejo cluster parked resolve <index> --reason "why" [--lineage <lineage>]
```

`resolve` refuses without a reason. It keeps every event of the entry, its
payload and last error, with who resolved it, when and why, in
`cluster_parked_resolution`, in the transaction that removes it from the
parked events; the resolution is also written to the replicated audit and
logged. Only then may the trim pass it.

## Backups hold the trim

Each checkpoint records the backup cursor; the event queue is never trimmed
past it, whatever its lag, since a PostgreSQL restored to that checkpoint
replays what follows from the log. When it lags more than 1,048,576 entries
(checkpoints stopped or failing), the held trim is counted
(`forgejo_cluster_consumer_backup_trim_held_total`), `/api/healthz` warns
under `cluster:backup_lag` and the administration page says "backup lags N
entries": take a checkpoint.

## The `[cluster]` keys

| Key | Default | Purpose |
|---|---|---|
| `ENABLED` | `false`, `true` when `EXPECTED_NODES` is 3 or more | Turns the replicator on. |
| `EXPECTED_NODES` | none (manual mode) | How many nodes the cluster has: 1, or 3 to 5. At 1 the section is off and Forgejo is the standard product. From 3 the node forms the cluster by itself: see "The autonomous mode". 2 is refused (no safe failover without a third vote), and so is any other value. |
| `DRIVER` | `kitsunium` | The consensus engine compiled into the binary. |
| `SLOT` | none, required | The node's slot, `[a-z0-9-]{1,32}`, the one its certificate names. |
| `CLUSTER_ID` | none | The cluster id, 32 lowercase hex digits, read by `cluster init`. |
| `LISTEN_ADDR` | none, required | The consensus listener, on the management address. A wildcard address is refused. |
| `OBJECT_PORT` | `7481` | The object channel port of every member. |
| `OBJECT_LISTEN_ADDR` | host of `LISTEN_ADDR` and `OBJECT_PORT` | The object channel listener, mutual TLS. |
| `OBJECT_PEERS` | empty | Per slot, `slot=host:port`, when members do not share `OBJECT_PORT` (several nodes on one host). |
| `PEERS` | empty | The seeds, comma-separated `slot=host:port`. The committed membership's addresses take over afterwards. |
| `PEER_PINS` | empty | For a node joining by itself: `slot=spki` of the members it trusts, as `forgejo cluster status` prints them. Its JOIN counts only an answer from one of them; without any it does not join. |
| `TLS_CERT_FILE` | none, required | The node's certificate. It may hold the key too. |
| `TLS_KEY_FILE` | `TLS_CERT_FILE` | The node's key. |
| `TLS_CA_FILE` | none, required | The cluster's CA. |
| `DATA_DIR` | `<APP_DATA_PATH>/cluster` | The log and snapshots, never on a tmpfs, and never restored from a backup: a restored directory comes back under the same id and incarnation with a log that went back, and an acknowledged write is lost. A lost directory is replaced by an empty one, which joins under a new id, or by `forgejo cluster restore`. `forgejo dump` leaves it out. |
| `HOOK_SOCKET` | `<APP_DATA_PATH>/cluster-run/hook.sock` | The private socket of the git hooks and of `forgejo cluster`. 103 bytes at most. |
| `OPERATORS` | empty | Bootstrap only: these site administrators get the operator permission while nobody holds it. |
| `STEP_UP_MAX_AGE` | `5m` | How long a re-authentication allows further operations. |
| `STEP_UP_OIDC_SOURCE`, `STEP_UP_ACR` | empty | The OpenID Connect source every operator re-authenticates with, and the authentication class required. Without a source, password and TOTP apply. |
| `STEP_UP_PASSWORD_OPERATORS` | empty | Break-glass logins that keep password and TOTP when `STEP_UP_OIDC_SOURCE` is set. |
| `READ_AFTER_WRITE_TIMEOUT` | `2s` | How long a request waits for the node to catch up with the caller's last write, and a git request (fetch, clone, push over HTTP or SSH) with the cluster's commit index. Without a known leader a git request does not wait: it is served as the node has it, except a repository whose directory on the node still holds another repository than the one the table holds under its name (a rename or a recreation of the name not projected there yet): git over HTTP and SSH answers 503 until the node projects it. |
| `HOOK_TIMEOUT` | `1m`, between `20s` and `1h` | How long one ref transaction may take in the cluster: its objects written on a majority, then its entry committed. A push that needs longer fails with nothing written; its retry ships only the objects the peers still lack. The proxies in front of Forgejo must allow at least as long. |
| `MAX_LOG_BYTES`, `MAX_SNAPSHOT_BYTES` | 1 GiB | Bounds of the log and of a snapshot. |
| `ACK_MARGIN` | `1024` | How far the trimming of consumed events lags the cursor. |
| `SQL_STANDALONE` | `false` | Declares that no replica of the database can ever be promoted (a single PostgreSQL, no standby): the durability check then asks for local durability only. Never set it under Patroni. |
| `TXN_TTL`, `MAX_TXN_PER_REPO`, `MAX_TXN_TOTAL` | `30s`, `64`, `4096` | Bounds of the git transactions in flight. |
| `JOIN_GRANT_TTL` | `1h` | How long an operator's permission to join lasts. |
| `DEAD_MEMBER_TIMEOUT` | `5m`, one minute at least | Autonomous mode: how long a member stays out of the leader's reach before it is removed. |
| `WITNESSES`, `WITNESS_KEYS` | empty | The Recover witnesses (one per witness host) and their Ed25519 public keys, in the same order. |
| `WITNESS_OVERRIDE_SHA256` | empty | The hash of the operator's boot override, read from `FORGEJO_CLUSTER_WITNESS_OVERRIDE`. |
| `RENEW_VAULT_ADDR` | empty | Vault, for the in-process certificate renewal. Off while empty. |
| `RENEW_VAULT_MOUNT`, `RENEW_VAULT_ROLE`, `RENEW_TTL` | `pki-consensus`, `SLOT`, `720h` | The PKI mount, the slot's role and the lifetime asked for. |
| `RENEW_VAULT_JWT_FILE` | empty | The Nomad allocation's workload identity, read again at each login. |
| `RENEW_VAULT_AUTH_MOUNT`, `RENEW_VAULT_AUTH_ROLE` | `jwt-consensus`, `SLOT` | Vault's JWT auth mount and the slot's role. |
| `RENEW_VAULT_TOKEN_FILE` | empty | A Vault token in a file, the fallback when the identity login fails. |
| `NOMAD_ADDR` | empty | Nomad's API, read-only, for the JOIN drain check: `http(s)://` or `unix://` (the task API socket). Off while empty. |
| `NOMAD_TOKEN_FILE` | empty | A `node:read` token, read again at each call. |
| `NOMAD_SLOT_META` | `forgejo_slot` | The Nomad node meta naming the slot it hosts. |
| `RENEW_VAULT_CA_FILE`, `NOMAD_CA_FILE` | empty | The CA of Vault's and Nomad's HTTPS when it is private. A plain `http://` address off the loopback is logged: tokens then rely on an encrypted network. |

## The autonomous mode

Set `EXPECTED_NODES` and `PEERS`, naming every other slot, the same list on
every node, and start the nodes. Nobody runs `cluster init`, `bootstrap` or
grants a join.

- **One node** (`EXPECTED_NODES = 1`) is plain Forgejo: no replicator, no
  data directory, the hooks upstream writes. A node whose data directory
  already holds a cluster's log refuses to start with 1, since alone it would
  fork the repositories; move the directory away on purpose, or set
  `ENABLED = false`, to run it standalone.
- **Two nodes are refused.** Two nodes cannot fail over safely without a
  third vote: when the link between them is cut, neither can tell a dead
  partner from a cut link, and if both kept writing, the repositories would
  split for good. Choose 1, or 3 and more; a third node may be small.
- **Forming.** Each node stamps its data directory and asks the others who
  they are, over the object channel with mutual TLS. If one is already a
  member, it joins. Otherwise the node with the lowest slot founds the
  cluster alone, once every expected node answers, and the others join it.
  A majority is not enough: a silent node may be the last one holding the
  log, and founding without it would start a second cluster under the same
  id. Every node must be given the same size and the same `PEERS`.
- **Voters.** `EXPECTED_NODES` sizes the founding and `PEERS`; afterwards
  every live member votes: three, four or five. The leader promotes a
  learner (the eldest first) as soon as it caught up. `forgejo cluster
  status` prints each member as voter, learner or removed, its rank (the
  order it entered the membership) and since when this node sees it so.
- **Dead members.** The leader removes a member it has not heard from for
  `DEAD_MEMBER_TIMEOUT`, as long as three voters remain without it. The
  removal is a committed membership change, so it happens only while a
  majority of the voters answers the leader. At five nodes: the first
  failure is removed (four voters), the second too (three voters); after
  the third, two of three voters are a majority and the cluster keeps
  writing, each write on two disks; after the fourth, it is read-only until
  a member returns. If a majority is lost at once, there is no leader:
  nothing is removed and nothing is written until a member returns.
- **A removed node that comes back** has an id the cluster retired. Once a
  majority of the voters of the newest membership confirm it, the node moves
  its consensus data directory aside (`<DATA_DIR>.retired-<UTC time>`, kept,
  never deleted), stamps a new id, joins as a learner through the CA
  admission, catches up, and is made a voter again. Its repositories are
  left as they are; the projector and the object channel bring them in line.
- **Joining** needs no grant: a node presenting a certificate of this
  cluster's CA, for a slot `PEERS` names and that has no member, is admitted
  as a learner. A cluster admits one member above its size, for a
  replacement standing by. A grant bound to another key still refuses,
  retired ids are still refused, and the Nomad drain check and the
  witnesses' boot check still apply. `CLUSTER_ID` may stay empty: the
  certificate names the cluster.

**What the CA is trusted with.** In this mode, possessing a certificate the
cluster's CA signed for a slot is enough to join an empty slot, and a joining
node trusts every member presenting such a certificate (set `PEER_PINS` to
trust given keys only). Keep the CA offline or in Vault, and sign one
certificate per expected slot.

**Revoking a node.** An operator's Remove or Deny pin of a member first
revokes its key: the revocation is a replicated entry, so every member
refuses that key at the JOIN, under any id and whatever grant the slot has,
and the object channel no longer admits it. The node comes back only with a
new key, which the CA signs. The autopilot's removal of a dead member revokes
nothing, so a node that was only out of reach returns by itself. The
revocation is a command of payload version 3, which an older binary cannot
read: while a member still runs one, the cluster writes an older version,
the Remove or Deny pin goes through without the revocation, and the
administration says the key is not revoked yet. The request is kept in the
log, and the revocation is written by itself once every member runs this
version (the autopilot raises the version; in the manual mode, the
operator raises it from the administration page). A JOIN grant is bound to the key it first admits:
that node may retry, another key does not join under it.

**Payload version 4** carries the pusher's name in each push entry. The
log consumer uses it to tell a pusher ID handed out again after a database
loss (another name, and no redirect from the old name to the account) from
the pusher itself, with no clock involved; before, it compared the push
node's clock with the account's creation time, and a skew of more than
five minutes parked a legitimate push. A binary of this version raises the
cluster straight to version 4, the revocation of version 3 included, once
every member runs it; entries written before keep the clock rule.

**A compromised CA** is replaced, never revoked in place. Sign new
certificates under a new CA, add the new CA to `TLS_CA_FILE` beside the old
one and roll the certificates; then remove the old CA from the file and
restart every node, one after the other. The consensus transport adds the
roots it reloads to the ones it holds and drops none until it restarts, so
until the last node restarted, a certificate of the old CA is still accepted
at the JOIN.

## Founding the cluster

This is the manual mode, without `EXPECTED_NODES`. The three servers are
stopped and share one configuration, apart from `SLOT`, `LISTEN_ADDR` and
the TLS files.

1. On each node, `forgejo cluster init`. It prepares the data directory and
   prints the member line `id:incarnation:slot:spki:host:port`.
2. Gather the three lines.
3. On each node, `forgejo cluster bootstrap --members "line1,line2,line3"`,
   with the same list everywhere. It is offline and may be run again.
4. Start the three servers. A leader is elected within a second or two.
5. If repositories existed before the cluster, run `forgejo cluster import`
   once on the node that holds them. A push to a ref that is not in the table
   is refused.
6. Grant the operator permission, through `OPERATORS` on the first start or
   with `forgejo cluster operator add <login>`.

## Adding or replacing a node

The engine refuses a slot that still has a member, and a Remove never takes
the voters below three. A replacement is therefore always an addition first.

- **A new slot.** Add the new node, promote it, then remove the old one. The
  cluster keeps its fault tolerance throughout.
- **The machine of a slot is replaced.** The new machine joins under a new
  slot name (for example `node-a2`, with a certificate naming it), is
  promoted, and the old member is removed: a slot that still has a member
  cannot be reused, and removing first would go below three voters. Replace
  one node at a time.

The steps for the node that joins:

1. On the new node, set `PEER_PINS` from `forgejo cluster status` on a
   member, run `forgejo cluster init`, then start the server.
2. Either:
   - in the administration, "Authorize a node to join" for its slot,
     optionally bound to its key (`slot:spki`): the node asks to join by
     itself and enters as a learner;
   - or "Add a learner" with the line `cluster init` printed.
3. `forgejo cluster objects verify` on the new node, then `backfill` if
   objects are missing.
4. "Promote" the learner once it caught up.
5. "Transfer leadership" if the old node leads, then "Remove" the old one.

## The administration page

Site Administration, then Cluster. Every site administrator sees it; only
cluster operators change anything.

- The node: role, term, leader, commit and applied index, lineage, fault
  tolerance, certificate expiry, table digest.
- The members: progress, lag and reachability of each. Only the leader knows
  these values: open the page on the leader.
- The alarms, including the no-space alarm and its release.
- The projection lag per repository.
- The replicated audit log of operations, with their operator.

Every operation asks to type its target again and a re-authentication. Once
`STEP_UP_OIDC_SOURCE` is set, every operator re-authenticates at that
provider (with the class `STEP_UP_ACR` when set), and a password is not
accepted. The identity expected at the provider is pinned on the operator
permission: the account's link when the permission is granted, or
`forgejo cluster operator pin <login>` on a node. Deleting or replacing the
link from the account settings changes nothing. Without an active source or a
pinned identity, the operation is refused. Only the break-glass accounts in
`STEP_UP_PASSWORD_OPERATORS` keep password and TOTP. An operation may be
submitted on any node, which forwards it to the leader. No API route leads to
it.

## Recovery

These commands run with the server stopped, on the data directory.

- `forgejo cluster inspect` prints the stamp, the log, the snapshots and any
  quarantine.
- `forgejo cluster quarantine clear --exit wipe-and-rejoin` or
  `--exit truncate-and-stay-silent` leaves a quarantine after a corrupt log.
- `forgejo cluster recover --new-cluster <id> --members ... --fenced id:method:receipt`
  forces a new configuration after the majority is lost. Every old voter left
  out must be fenced, and every fence carries a receipt signed by each
  witness. `--members` names this node alone: it is the only voter of the
  new cluster, the other survivors enter as learners (`--joining`). Before
  anything, the command probes each old member at its consensus address
  (`PEERS`, and `--old-peer slot=host:port`) and refuses while one answers:
  the witnesses fence the old cluster at its next boot, they do not stop a
  member that still runs. `--force-old-members-answer` overrides it and is
  logged. The command asks to type the new id again.
- `forgejo cluster snapshot save --out <file>`, server running, writes the
  table snapshot with its metadata beside it, and `<file>.sha256`: the
  SHA-256 of the file, then of `<file>.meta` (`sha256sum -c` checks both).
- `forgejo cluster restore --file <file> --sha256 <digest> --new-cluster <id> --members ...`
  restores that snapshot into an empty directory, on every node of the new
  cluster. `<digest>` is the file's, the first line of `<file>.sha256`, as
  `snapshot save` logged it: read it from the authenticated backup store.
  The file is refused when it differs, and so is `<file>.meta` when it
  differs from its line in `<file>.sha256`.
  Like `recover`, it founds the new cluster only once the old one cannot
  come back: `--fenced id:method:receipt-file[+receipt-file]` for every
  voter the snapshot's cluster had at the cut, signed by both witnesses
  (`--witness-key` twice, or `WITNESS_KEYS`), and no old member answering
  the probe (`PEERS`, `--old-peer slot=host:port`, or
  `--force-old-members-answer`, logged). The old cluster is the snapshot's;
  `--old-cluster <id>` names another one when the cluster still running
  succeeded it. Without the fence, the old site, back up, forms its
  cluster again and two forges write.
- After a restore or a PostgreSQL failover, `forgejo cluster reconcile` lists
  the differences between the table and SQL. `--apply` rebuilds the rows that
  can be regenerated (branches, tags); the others are reported with what to
  do, such as a pull request marked merged whose commit is missing from its
  base branch.
- A repository the table holds alive and SQL does not know is reported by
  `reconcile` with the index of its last change and since when it is
  unchanged. `reconcile`, `--apply` included, never deletes it. After a loss
  on the SQL side (an asynchronous PostgreSQL failover, a point-in-time
  recovery, `pg_rewind`, a backup restore) the table and the files every
  member kept are the repository's ONLY copy: restore it. Recreate its owner
  if SQL lost it too, then adopt the repository (Site administration >
  Repositories > Unadopted, or `POST /api/v1/admin/unadopted/{owner}/{repo}`).
  Never use "delete unadopted" in a cluster: it removes one node's files and
  the projector brings them back. "Delete all repositories missing their Git
  files" and "Reinitialize all missing Git repositories" are not offered in
  a cluster, and refused: they judge a repository missing from one node's
  disk, which a node behind on its projection is, and would delete it, or
  rewrite its HEAD, on every member.
- Only a deletion or a failed creation the cluster did not record is
  deleted, explicitly, one repository at a time:
  `forgejo cluster reconcile --delete-orphan <owner>/<repo> --expect-index <index> --reason "..."`.
  It is refused unless this node's table is confirmed current, the
  repository did not change since the index given, and SQL, read last, still
  has no row for it. The decision, its operator and its reason are in the
  audit.
- `forgejo cluster recover --from-observer <checkpoint> ...` rebuilds a
  cluster from an observer's protected checkpoint, every member lost
  ([observer.md](observer.md)). It needs the same `--fenced` receipts of
  both witnesses for every voter the checkpoint names, and the same probe
  of the old members.

## Observers

An observer (`[cluster] ROLE = observer`) is never a member and opens no
port: it pulls, over SSH, checkpoints that pair the refs, the objects, the
stored files and Forgejo's PostgreSQL at one causal cut, each one protected
by a snapshot the cluster cannot reach. The members take the checkpoints
once `CHECKPOINT_INTERVAL` is set. Setup, guarantees, limits and the
recovery: [observer.md](observer.md).

## Metrics

With `[metrics] ENABLED = true`, `/metrics` exposes two families.

- `consensus_*`, the engine's: role, term, fault tolerance, commit, apply
  and fsync latencies, and on the leader `consensus_peer_match_lag_entries`,
  `consensus_peer_unreachable` and `consensus_peer_last_contact_seconds` per
  peer.
- `forgejo_cluster_*`, Forgejo's: readiness, pending projection and the
  repositories whose projection keeps failing
  (`projection_failing_repositories`, alert when it stays above zero), event queue,
  consumer lag and stalls, parked events (`consumer_parked_events_total`,
  `consumer_unparked_events_total`), trims held for durability
  (`consumer_trim_held_total`), cursor regressions, git transactions in
  flight, certificate expiry,
  transport refusals before TLS, and the counters of proposals, object
  shipments and consumed events. In the autonomous mode,
  `forgejo_cluster_expected_nodes` (the founding size) and
  `forgejo_cluster_voters`.

`/api/healthz` says whether the node is ready. On the leader it gives the
fault tolerance, the gate of a node-by-node upgrade. A healthy follower
reports nothing.

## Under Nomad

`contrib/cluster/deploy/nomad` holds a generic job. Its shape:

- One job, three groups, each pinned on its machine's `${node.unique.id}`, a
  task named after its slot. Nomad never moves a member by itself:
  `reschedule { attempts = 0, unlimited = false }` and
  `disconnect { replace = false }`.
- **The witness check before Forgejo.** A `prestart` task runs
  `forgejo cluster witness check` through the image's setup, so it reads the
  same `[cluster]` settings as Forgejo, on the data volume mounted read-write
  (opening the data directory takes its lock file). It exits 77 when a witness
  says the lineage was superseded, or when none answers: Forgejo does not
  start, the allocation fails and Nomad does not reschedule it.
- **Vault through the workload identity.** The renewer logs in at
  `auth/jwt-consensus/login` with its slot's role and the JWT Nomad writes into
  the task's secrets. It keeps the token to two thirds of its TTL, then logs in
  again; a token Vault refuses is dropped and the signature retried once.
  `RENEW_VAULT_TOKEN_FILE` stays the logged fallback. On Vault's side, each
  role binds `nomad_namespace`, `nomad_job_id` and `nomad_task`, and its
  policy signs for its slot only.
- **The JOIN drain check.** Before admitting a node that joins by itself,
  Forgejo reads Nomad read-only (`GET /v1/nodes`, `GET /v1/node/<id>`,
  `node:read`). The node hosting the slot is the one whose `forgejo_slot` meta
  names it. The JOIN is refused while that node is draining, ineligible or not
  ready, and when Nomad does not answer. When several nodes carry the slot,
  the joiner's address decides; otherwise the refusal stands. The joiner reads
  the reason in its log.
- **Certificate reload.** A renewed certificate is used from the next
  handshake, on both sides. The object channel rotates its open connections as
  soon as the files change: idle ones close at once, a request in flight
  finishes on its connection, which then closes. The consensus transport keeps
  its connections to their natural end: the key does not change, so neither
  does the SPKI pin, and the engine already bounds each connection by the
  expiry of the certificate that opened it.

The lab proves this against a real Nomad: `make nomad-e2e` in
`contrib/cluster-lab`.

## Upgrades and release notices

Every official release of the newest Forgejo major gets a forgejo-raft
release built on it, `vX.Y.Z-raft.N` (`.github/README.md`, Releases): v17
today, and only 18 once 18.0.0 ships. Older majors, LTS included, get no
new raft release, so a cluster stays on the newest major to keep receiving
fixes. Another release is built only on request. The admin dashboard tells
when one is out: next to Forgejo's notice of a new official release, it
says whether the raft build of that release is available yet. Upgrade to
the raft build, never to the official binary, which has no cluster mode.

```ini
[cron.update_checker]
; default, empty disables the raft check only
RAFT_ENDPOINT = https://raw.githubusercontent.com/kodflow/forgejo-raft/release-index/releases.json
```

Outside Forgejo: the feed https://github.com/kodflow/forgejo-raft/releases.atom,
Watch, Custom, Releases on GitHub, or the tags of
`ghcr.io/kodflow/forgejo-raft`.

## Known limits

- A push must reach a majority of the nodes, objects included, within
  `HOOK_TIMEOUT` less ten seconds (fifty seconds by default). A very large
  push over a slow link may be refused; its shipment goes on in the
  background for five minutes, and a retry skips the members that already
  hold its objects. Raise `HOOK_TIMEOUT` for larger pushes. A first large
  repository goes through `forgejo cluster import`, which has its own
  30-minute budget, like `reconcile`, `objects backfill` and
  `snapshot save`.
- A push is committed whole or not at all. With git 2.54 and later, a
  non-atomic push of several refs is one git transaction that may reject
  some refs; the cluster cannot commit only the others, so when git would
  reject one ref (another writer's lock, a lock left behind for more than
  two minutes, a ref that moved on this node, a name that conflicts with
  one of this node's refs as a directory and a file) the whole push is
  refused and the client reads every ref rejected. Pushing again succeeds
  once the other writer is done. A lock left behind by a crashed git
  (a ref's, `HEAD.lock`, `packed-refs.lock`, `packed-refs.new`) is
  removed by the projector the next time it writes that ref, once no git
  of the repository is registered and the lock is older than both
  `TXN_TTL` and two minutes (counted in
  `forgejo_cluster_stale_locks_removed_total`); a lock on a ref the
  projector has nothing to write to is still removed by hand. An empty
  lock younger than two minutes, on a ref the push deletes, cannot be told
  from git's own.
- A deleted or renamed repository leaves its key in the replicated table
  for good: tombstoned refs, a deletion mark, a rename mark. Each costs a
  few hundred bytes in every snapshot, and the projector's first pass
  after a restart visits every key. Purging them changes the state machine
  (a new command and payload version, every node purging at the same
  index, the push-into-a-deleted-key refusal and the consumer's rename
  tracking kept intact for events still queued), so it is not done yet:
  an instance that deletes repositories by the hundred thousand should
  watch the snapshot size.
- A ref name must be valid UTF-8. git accepts other bytes, which the
  replicated table, in JSON, cannot carry unchanged.
- The object channel accepts what Forgejo accepts on a push: the fsck
  checks Forgejo ignores (`badTimezone`, `missingSpaceBeforeDate`,
  `zeroPaddedFilemode`, and any `receive.fsck.*` of `[git.config]`) are
  ignored there too. A transfer between members that makes no progress for
  two minutes is cut, and the next member is asked.
- The drain check covers the joiner's Nomad node. That the old member was
  drained or purged remains the responsibility of the operator who grants the
  join.
- Forgejo has no tracing: the engine's spans are not exported.
