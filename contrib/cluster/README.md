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

A parked event of another lineage than the table's (one a Recover, a
restore or a recovery from an observer ended) is also followed to its
repository through SQL's redirects: the table may not hold the renames it
went through, and a recovery from an observer starts a new table with none.
The table alone judges the events of its own lineage.

A parked event whose repository the leader alone cannot project (its own
copy damaged while the other members project it) is applied by no retry,
nor by `reconcile --apply`, for as long as that node leads, and the whole
queue waits behind it. The leader logs it once, `cluster:parked_events`
names it, and `forgejo cluster parked list`, asked on the leader, says it
under `waits`. Once the leader's retries have found it so for five minutes,
the autopilot of the autonomous mode hands the leadership to a voter that
answers, on its object channel, that it projects the repository at the
event's index, at most once every five minutes, and that voter applies it.
It never hands it to a voter that does not say so, a member on an older
binary included: the leadership does not go round for a repository no
member projects. In the manual mode, transfer the leadership to a member
that projects it: the leader's hello says, from then on, that it could not
derive the SQL, so the leadership is not handed back to it for its
checkpoints within the hour.

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
replays what follows from the log. Only the next checkpoint moves it, and
only the leader takes checkpoints, when its own `CHECKPOINT_INTERVAL` is
set. Pushes are refused once the queue holds 90% of its 64 MiB. Long
before that, once the backup holds a quarter of the queue (16 MiB) behind
it, or lags 1,048,576 entries, the held trim is counted
(`forgejo_cluster_consumer_backup_trim_held_total`) and logged,
`/api/healthz` warns under `cluster:backup_lag`, and the administration
page says how much the backup holds. `cluster:checkpoints` warns as soon as
the leader takes no checkpoint while a backup cursor of its lineage exists,
and `cluster:event_queue` names what holds the trim.

The leader is therefore a member that takes the checkpoints whenever a
voter does, in both modes. A leader whose `CHECKPOINT_INTERVAL` is 0 hands
the leadership to a voter whose `CHECKPOINT_INTERVAL` is set (each member
says it to the others), once that voter answers and is caught up, at most
once every five minutes. A leader that takes them never hands the
leadership over for them, so it does not go back and forth; and a member
that gave the leadership away because it could not derive the SQL is not
handed it back for its checkpoints within the hour. In the manual mode,
nothing makes a leader that cannot derive the SQL give the leadership
away: transfer it away from it. A member whose consumer kept failing while
it led then says so until it leads again and derives the SQL, so the
leadership is not handed back to it and the transfer stands. Once its
database is repaired, transfer the leadership to it, or restart it.
`cluster:checkpoints` says which voters take them and, for each one the
leader does not hand the leadership to yet, why (behind, out of contact,
receiving a snapshot, could not derive the SQL); or that none does.

A voter whose `CHECKPOINT_INTERVAL` is 0 does not start when the backup
cursor of its lineage exists and every other voter answers that it takes
none either: no checkpoint would ever close again. Set
`CHECKPOINT_INTERVAL` on it, or on another voter first. It starts whenever
it is not certain of that: a voter that does not answer, a member on an
older binary, a database it cannot read.

The way out of a held trim is a checkpoint that closes. Set
`CHECKPOINT_INTERVAL` on a voter: the leadership goes to it, or set it on
the leader and restart it. When a leader that takes them fails its
checkpoints, its log says why (`cluster checkpoint:`). Nothing else
releases the trim: once a checkpoint closed, keep `CHECKPOINT_INTERVAL` set
on at least one voter.

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
| `DATA_DIR` | `<APP_DATA_PATH>/cluster` | The log and snapshots, never on a tmpfs, and never restored from a backup: a restored directory comes back under the same id and incarnation with a log that went back, and an acknowledged write is lost. A lost directory is replaced by an empty one: the node comes back under a new id and with a new key (see [A node that comes back](#a-node-that-comes-back)), or by `forgejo cluster restore`. `forgejo dump` leaves it out. |
| `HOOK_SOCKET` | `<APP_DATA_PATH>/cluster-run/hook.sock` | The private socket of the git hooks and of `forgejo cluster`. 103 bytes at most. |
| `OPERATORS` | empty | Bootstrap only: these site administrators get the operator permission while nobody holds it. |
| `STEP_UP_MAX_AGE` | `5m` | How long a re-authentication allows further operations. |
| `STEP_UP_OIDC_SOURCE`, `STEP_UP_ACR` | empty | The OpenID Connect source every operator re-authenticates with, and the authentication class required. Without a source, password and TOTP apply. |
| `STEP_UP_PASSWORD_OPERATORS` | empty | Break-glass logins that keep password and TOTP when `STEP_UP_OIDC_SOURCE` is set. |
| `READ_AFTER_WRITE_TIMEOUT` | `2s` | How long a request waits for the node to catch up with the caller's last write, and a git request (fetch, clone, push over HTTP or SSH) with the cluster's commit index. Without a known leader a git request does not wait: it is served as the node has it, except a repository whose directory on the node still holds another repository than the one the table holds under its name (a rename or a recreation of the name not projected there yet, or a directory the projector stamped with another repository ID than the database's, which a node cut off from the others also sees): git over HTTP and SSH, the repository's web pages and its API answer 503 until the node projects it. A directory stamped before this check, or a repository no push carrying its ID wrote yet, is judged by the table alone. A repository generated from a template with its git content or its git hooks, through the web form or the API, first waits for the node to serve the template at the cluster's commit index, and answers 503 with nothing created while the node does not, past this bound or without a leader, or while the template's directory still holds another repository. So does a fork, through the web form or the API, for its base: the node copies the base's directory outside the repository root, checked before and after, then takes the fork's refs from that copy, and nothing of the fork is written before. |
| `HOOK_TIMEOUT` | `1m`, between `20s` and `1h` | How long one ref transaction may take in the cluster: its objects written on a majority, then its entry committed. A push that needs longer fails with nothing written; its retry ships only the objects the peers still lack. The proxies in front of Forgejo must allow at least as long. |
| `MAX_LOG_BYTES`, `MAX_SNAPSHOT_BYTES` | 1 GiB | Bounds of the log and of a snapshot. |
| `ACK_MARGIN` | `1024` | How far the trimming of consumed events lags the cursor. |
| `SQL_STANDALONE` | `false` | Declares that no replica of the database can ever be promoted (a single PostgreSQL, no standby): the durability check then asks for local durability only. Never set it under Patroni. |
| `TXN_TTL`, `MAX_TXN_PER_REPO`, `MAX_TXN_TOTAL` | `30s`, `64`, `4096` | Bounds of the git transactions in flight. |
| `JOIN_GRANT_TTL` | `1h` | How long an operator's permission to join lasts. |
| `DEAD_MEMBER_TIMEOUT` | `5m`, one minute at least | Autonomous mode: how long a member stays out of the leader's reach before it is removed. |
| `WITNESSES`, `WITNESS_KEYS` | empty | The Recover witnesses (one per witness host) and their Ed25519 public keys, in the same order. |
| `WITNESS_OVERRIDE_SHA256` | empty | The hash of the operator's boot override, read from `FORGEJO_CLUSTER_WITNESS_OVERRIDE`. |
| `RENEW_VAULT_ADDR` | empty | Vault, for the in-process certificate renewal, and for the new key a node takes before each new id it stamps. Off while empty: an operator then installs that key. |
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
  `DEAD_MEMBER_TIMEOUT`, as long as three voters remain without it, and
  marks its key, which never joins again (from payload version 7). The
  removal is a committed membership change, so it happens only while a
  majority of the voters answers the leader. At five nodes: the first
  failure is removed (four voters), the second too (three voters); after
  the third, two of three voters are a majority and the cluster keeps
  writing, each write on two disks; after the fourth, it is read-only until
  a member returns. If a majority is lost at once, there is no leader:
  nothing is removed and nothing is written until a member returns.
- **A removed node that comes back** has an id the cluster retired. Once a
  majority of the voters of the newest membership confirm it, the node takes
  a new key, moves its consensus data directory aside
  (`<DATA_DIR>.retired-<UTC time>`, kept, never deleted), stamps a new id,
  joins as a learner through the CA admission, catches up, and is made a
  voter again. Its repositories are left as they are; the projector and the
  object channel bring them in line. See
  [A node that comes back](#a-node-that-comes-back).
- **A voter of three whose data directory is lost** comes back on an empty
  directory under a new id, while its old id still holds the slot: the
  engine refuses its JOIN, and the floor of three voters keeps the old id
  from being removed. The leader replaces it by itself, once the old id has
  been out of its reach for `DEAD_MEMBER_TIMEOUT`, the node at the slot's
  address in `PEERS` answers as joining under a certificate of the CA for
  that slot, and its key is new: neither a member's, nor revoked, nor asked
  for revocation. The leader adds the new id as a learner under the same
  slot, promotes it once it caught up (four voters), then removes the old
  id (three again): the membership never lists fewer than three voters.
  Fault tolerance stays at 0 from the moment the disk is lost until the old
  id is removed, since four voters with one dead need the three others; a
  change of leader in between counts `DEAD_MEMBER_TIMEOUT` again. A
  replacement the leader added and then removed before it was ever heard
  stamps another id, with a new key, once the members say its id is
  retired.
- **Joining** needs no grant: a node presenting a certificate of this
  cluster's CA, for a slot `PEERS` names and that has no member, is admitted
  as a learner. A cluster admits one member above its size, for a
  replacement standing by. A grant bound to another key still refuses,
  retired ids are still refused, and the Nomad drain check and the
  witnesses' boot check still apply. `CLUSTER_ID` may stay empty: the
  certificate names the cluster, at start-up and to the in-process
  renewer, which asks the signer for the URI SAN the certificate in use
  names. Set, `CLUSTER_ID` must match that certificate, as at start-up,
  and `SLOT` too: a mismatch refuses every renewal before the signer is
  asked. A URI SAN with no cluster is never asked for.

**What the CA is trusted with.** In this mode, possessing a certificate the
cluster's CA signed for a slot is enough to join an empty slot, and a joining
node trusts every member presenting such a certificate (set `PEER_PINS` to
trust given keys only). Keep the CA offline or in Vault, and sign one
certificate per expected slot.

**Revoking a node.** An operator's Remove or Deny pin of a member first
revokes its key: the revocation is a replicated entry, so every member
refuses that key at the JOIN, under any id and whatever grant the slot has,
and the object channel no longer admits it. The node comes back only with a
new key, which the CA signs. The autopilot's removal of a dead member marks
its key the same way, from payload version 7: a key serves one node id for
the life of the cluster, and a node that was only out of reach returns by
itself under a new key (see [A node that comes back](#a-node-that-comes-back)).
Below version 7 the autopilot asks for nothing, since a member's binary
could not withdraw a request the engine refuses: a node that rejoins after
its removal still takes a new key, by its own rule, but one that comes back
on an empty data directory without `RENEW_VAULT_ADDR` keeps the key in
place, which the cluster then does not refuse. In every case the revocation is written
only once no member holds the key, so a member that stays is never cut off.
The revocation is a command of payload version 3, which an older binary cannot
read: while a member still runs one, the cluster writes an older version,
the Remove or Deny pin goes through without the revocation, and the
administration says the key is not revoked yet. The request is kept in the
log, and the revocation is written by itself once every member runs this
version (the autopilot raises the version; in the manual mode, the
operator raises it from the administration page). A JOIN grant is bound to the key it first admits:
that node may retry, another key does not join under it.

**Payload version 9** refuses the rename or the transfer of a repository
whose name the cluster no longer holds: deleted, or moved away by the
rename of its owner. A request loaded before an owner's rename committed
through another node, and served by a node whose disk was still behind
that rename, moved a directory the cluster had already moved elsewhere:
the cluster took a rename that moved nothing, the database committed the
new name, and every node served the repository empty under it while its
git data stayed under a name the database did not know. At every version
the node serving the request now reads the repository's name in its
table, once it applied the log as far as a majority confirms, before
Forgejo writes or moves anything, and again before it proposes the move;
the refusal is certain, and Forgejo moves the directories back. From
version 9 the entry itself is refused when an owner's rename commits
between that check and the entry. Until every member runs a binary of
version 9 and the version is raised, that last race is taken as before.

**Payload version 8** renames an owner by its names. Before, the rename of
a user or an organization was one entry listing every repository of the
owner, wikis included, and the engine refuses an entry past 1 MiB: an
owner of about 6 600 to 7 400 repositories with wikis could never be
renamed, and the rename was refused before anything was written. From
version 8 the entry names the old and the new owner alone, and each member
moves, when it applies the entry, every repository its table holds under
the old owner at that point of the log: the same moves as the entry
listing them, so a member behind the log, or one restored from a snapshot
or a checkpoint, ends up with the same table. The rename has no size limit
any more. Until every member runs a binary of version 8 and the version is
raised, an owner's rename is the entry listing its repositories, refused
past the bound as before.

**Payload version 7** makes a Remove or Deny pin that the engine refuses
for certain leave no effect. The revocation is asked for before the
change, so that the removed node never joins again in between. When the
engine refuses the change (another change in flight, no answering
majority without the member), the request is withdrawn, and the key is
only what the member's next departure makes of it: revoked again if an
operator removes it, marked if the autopilot removes it as a dead member.
Before version 7 the request stayed, and that member was refused for good. A change whose outcome is
unknown keeps its request, written once no member holds the key. The
autopilot neither promotes nor keeps a learner whose key is asked for
revocation. A request made before the raise stays as it was.

**Payload version 4** carries the pusher's name in each push entry. The
log consumer uses it to tell a pusher ID handed out again after a database
loss (another name, and no redirect from the old name to the account) from
the pusher itself, with no clock involved; before, it compared the push
node's clock with the account's creation time, and a skew of more than
five minutes parked a legitimate push. A binary of this version raises the
cluster straight to version 4, the revocation of version 3 included, once
every member runs it; entries written before keep the clock rule.

**The engine's cluster version** is the consensus protocol every member
must speak, apart from the payload version above. The leader raises it to
protocol 2 by itself, in both modes, once every member announces it: a
cluster founded at 0 never gets the protections of protocol 2, among them
the record that lets a voter stopped while another member was admitted and
made leader hear that leader again. `forgejo cluster status` shows it. The
raise waits for a member still on a binary of protocol 1 and is never
undone: a member rolled back to such a binary afterwards stops instead of
forking.

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

**The data directory of a voter is lost** (a disk replaced, or
`forgejo cluster quarantine clear --exit wipe-and-rejoin`). The node keeps
its slot and comes back under a new id and a new key, while its old id
still holds the slot: the JOIN of the new id is refused, and at three
voters the old id cannot be removed first. In the autonomous mode the
leader replaces it by itself (see [The autonomous mode](#the-autonomous-mode)).
Otherwise, in the manual mode, or when the autopilot does not (a joiner on
an older binary, a slot `PEERS` does not name, a key the cluster refuses,
which its leader logs), replace it by hand, addition first:

1. Stop the node and give it a new key (see
   [A node that comes back](#a-node-that-comes-back)).
2. On its empty data directory, `forgejo cluster init` (with `--cluster`
   when `CLUSTER_ID` is empty); it prints the member line, then start the
   server.
3. In the administration, "Add a learner" with that line: the engine adds
   it under the slot the old id still holds.
4. "Promote" it once it caught up: four voters.
5. "Remove" the old id: three voters again, and its key is revoked.

In the autonomous mode the autopilot does steps 4 and 5 once the learner
is added. The cluster never lists fewer than three voters.

## A node that comes back

A key serves one node id for the life of the cluster. A node that takes a
new id takes a new key first, whatever the reason: its data directory was
lost or wiped, the autopilot removed it as a dead member and it rejoins, or
it replaces a lost voter of three. The cluster refuses the old key at the
JOIN, under any id: an operator's Remove or Deny pin revokes it, and the
autopilot's removal of a dead member marks it (from payload version 7). A
node that came back with the key of a removed member would otherwise be
refused as leader by every node whose configuration still lists the
removed member under that key (kitsunium/sdk#648); from the engine's
protocol 3, the engine refuses that key too.

**With `RENEW_VAULT_ADDR`** the node does it by itself. Before it stamps a
new id (at start-up on an empty data directory in the autonomous mode,
before it rejoins under a new id, and in `forgejo cluster init`), it makes
a new key on the host, of the kind of the old one, has Vault sign it for
its slot under the same URI SAN, checks the certificate as a renewal does,
and installs both. While Vault refuses, nothing is stamped and the node
asks again; at start-up its hello says meanwhile that it has not read its
data directory, so nothing is founded without it. A key kept in a file of
its own is replaced key first, both written aside beforehand: a stop in
between is completed at the next start, and a stop before leaves the old
pair whole. This includes the first start of a founder.

**Without `RENEW_VAULT_ADDR`** (cert-manager, keys installed by hand) the
operator gives the node its new key:

1. Make a new key and have the CA sign it for the slot: with cert-manager,
   delete the node's Secret, so that it draws a new key despite
   `rotationPolicy: Never`; by hand, as for the first certificate.
2. Install the pair in `TLS_KEY_FILE` and `TLS_CERT_FILE`.
3. A node the autopilot removed does not rejoin until it reads a key other
   than the one its retired id held, and logs why; it rejoins as soon as it
   reads one, restarted or not. A
   node on an empty data directory stamps its new id with the key in
   place, which the cluster refuses when a member holds or held it:
   restart it once the new key is installed. Its consensus engine keeps
   the key it opened with, so only a restart makes it present the new one,
   and until then the leader does not take it for the replacement of its
   slot.

A node that stamped its new id under its old key, by an older binary or
before `RENEW_VAULT_ADDR` was set, keeps it: install a new key and restart
it, or empty its data directory again once `RENEW_VAULT_ADDR` is set.
Update `PEER_PINS` on the nodes that pin a key that changed.

While a member still runs a binary from before this rule, the autopilot of
a leader on this version still marks the key of a dead member it removes:
a node of the older binary removed so does not come back with its old key.
Upgrade it, and empty its data directory or give it a new key.

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
  forces a new configuration after the majority is lost. Every old voter but
  this node must be fenced, and every fence carries a receipt signed by each
  witness. `--members` names this node alone: it is the only voter of the
  new cluster. The other survivors do not keep their old node ID: each
  leaves the old cluster first (`forgejo cluster quarantine clear --exit
  wipe-and-rejoin`, or an empty data directory), runs `forgejo cluster init
  --cluster <new id>`, and the member line it prints goes into `--joining`:
  they enter as learners, under a new node ID. A member under the ID of an
  old or retired member, or two members under one ID, is refused. Before
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
  cluster. `<digest>` is the file's: the 64 hex digits that open the first
  line of `<file>.sha256`, without the file name that follows them, as
  `snapshot save` logged it; read it from the authenticated backup store.
  The file is refused when it differs, and so is `<file>.meta` when it
  differs from its line in `<file>.sha256`.
  Like `recover`, it founds the new cluster only once the old one cannot
  come back: `--fenced id:method:receipt-file[+receipt-file]` for every
  voter the snapshot's cluster had at the cut, signed by both witnesses
  (`--witness-key` twice, or `WITNESS_KEYS`), and no old member answering
  the probe (`PEERS`, `--old-peer slot=host:port`, or
  `--force-old-members-answer`, logged). The old cluster is the snapshot's;
  `--old-cluster <id>` names another one when the cluster still running
  succeeded it. The fence is then that cluster's: receipts the witnesses
  signed for `<id>`, given for every voter it has (the command no longer
  knows them, and requires at least one), never receipts for the
  snapshot's cluster. Without the fence, the old site, back up, forms its
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
  member kept are the repository's ONLY copy: restore it. An observer's
  checkpoint holds it too, and `recover --from-observer` restores it as an
  orphan ([observer.md](observer.md), step 6). Recreate its owner
  if SQL lost it too, then adopt the repository (Site administration >
  Repositories > Unadopted, or `POST /api/v1/admin/unadopted/{owner}/{repo}`).
  The adoption writes the repository's HEAD, and its wiki's, again under the
  new row's ID, which every member then binds to the directory: every node
  refuses to serve a directory stamped with another ID than the database's,
  and the lost row's ID stays in the stamps until a write carries the new one.
  That write is part of the adoption: when the cluster refuses it or does not
  answer (no quorum, a leader change), the adoption fails and is rolled back,
  and the repository stays in the unadopted list, to adopt again once the
  cluster has a leader.
  "Delete unadopted" (Site administration > Repositories > Unadopted,
  `DELETE /api/v1/admin/unadopted/{owner}/{repo}`, or a user's own settings
  with `ALLOW_DELETION_OF_UNADOPTED_REPOSITORIES`) is refused for a
  repository the table holds, and the refusal names the
  `reconcile --delete-orphan` that deletes it from the cluster. It used to remove the
  files of the node serving the request only: the table kept the
  repository, the other members their copy, and nothing brought that
  node's copy back. Files the table does not hold, on one node's disk
  alone (an init the cluster never recorded), are still deleted. A node
  that lost its copy of a repository the table holds, removed by hand or by
  "delete unadopted" before this release, projects it again from the other
  members at its next start. `forgejo cluster reproject <owner>/<repo>.git`,
  run on that node, schedules that projection without a restart: the
  node's projector takes it at its next pass, and the copy is back once it
  fetched the objects from the other members.
  A repository is not created, forked, generated from a template or
  migrated under a name the table holds a repository under, its own or its
  wiki's: the node serving the request reads its table once it applied the
  log as far as a majority confirms, before anything is written, and the
  creation fails as for files already on its disk, whatever its own disk
  holds. Before, a node that had lost its copy, or not projected it yet,
  found the name free, and the new repository took the orphan's key: it
  served the deleted repository's refs and objects under its own rights,
  or every node refused it for the orphan's ID. Adopt the orphan, or
  delete it with `reconcile --delete-orphan`, first. A `git init` that
  reaches the cluster with no such check, under a key holding refs, is
  refused too, and the orphan is left as it was.
  "Delete all repositories missing their Git files" and "Reinitialize all
  missing Git repositories" are not offered in a cluster, and refused: they
  judge a repository missing from one node's disk, which a node behind on
  its projection is, and would delete it, or rewrite its HEAD, on every
  member.
- Only a deletion or a failed creation the cluster did not record is
  deleted, explicitly, one repository at a time:
  `forgejo cluster reconcile --delete-orphan <owner>/<repo> --expect-index <index> --reason "..."`.
  It is refused unless this node's table is confirmed current, the
  repository did not change since the index given, and SQL, read last, still
  has no row for it. The decision, its operator and its reason are in the
  audit.
- A deletion is two entries, the wiki's first, the repository's only once the
  wiki's committed. When the wiki's is lost, both stay; when the repository's
  is, the repository stays without its wiki. Either way it is an orphan as
  above. A wiki left alive by an older deletion belongs to the deleted
  repository: no repository is created again under the name while it stays,
  and one created beside it before this release is refused by every node
  while it stays. `reconcile` reports it (`"kind": "wiki"`), and only it is deleted
  by `forgejo cluster reconcile --delete-orphan <owner>/<repo>.wiki --expect-index <index> --reason "..."`,
  refused when it is the wiki of the repository SQL has.
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
  says the lineage was superseded: Forgejo does not start, the allocation
  fails and Nomad does not reschedule it. While no witness answers, it waits
  and asks again (5 s, doubled up to a minute), and Forgejo starts once one
  says the lineage is clear; Forgejo's own boot check does the same. It
  exits 78 when the witness settings are wrong.
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
  background for five minutes, or until the request's own deadline when
  that is later, and a retry skips the members that already hold its
  objects. Raise `HOOK_TIMEOUT` for larger pushes, up to the
  30 minutes a member gives one incoming pack. A first large
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
  for good: tombstoned refs, a deletion mark, up to 16 rename marks. Each costs a
  few hundred bytes in every snapshot, and the projector's first pass
  after a restart visits every key. Purging them changes the state machine
  (a new command and payload version, every node purging at the same
  index, the push-into-a-deleted-key refusal and the consumer's rename
  tracking kept intact for events still queued), so it is not done yet:
  an instance that deletes repositories by the hundred thousand should
  watch the snapshot size.
- One git transaction updates at most 4096 refs. A larger one, a
  `push --mirror` or `push --tags` of thousands of refs with git 2.54 and
  later, is refused before its objects are shipped: push in several
  smaller pushes. A fork and a migration take their refs by batches of
  512, whatever their number.
- A mirror's remote is a node's git config, which the cluster does not
  replicate: a node that lacks it recreates it from the database before
  each mirror operation, without any password, and a node that holds it
  brings it in line with the database at each sync: a pull mirror's
  address, its wiki's included, and a push mirror's branch filter, both
  changed through one node only. A node whose remote already matches
  rewrites nothing and asks no remote. The database keeps a
  mirror's password encrypted, and every node passes it to git through a
  credential helper, never through its git config. A push mirror created
  before this release has its password stored by the node that created
  it, at its first sync there; until then it pushes from that node only.
  `forgejo doctor` never removes a push mirror in a cluster because one
  node lacks its remote.
- A push mirror's sync pushes `--mirror --force` from the node's disk. A
  node pushes only once it serves the repository, and its wiki, at the
  commit index the cluster confirms, from a directory that holds the
  repository the database names; otherwise the sync fails, is not
  recorded as done, and the next `update_mirrors` round retries it. A
  node holds the sync in the database while it runs: `push_mirror`'s
  `last_update` is then the hold's expiry, some minutes ahead, which the
  settings page and the API show meanwhile. Two nodes' pushes never
  overlap, and each starts from a state at least as recent as the
  previous one. This assumes the nodes' clocks agree within a minute, a
  database that keeps its commits across a failover, and a remote that
  applies a push within a minute of the client's `[git.timeout] MIRROR`;
  a node from before this release does not hold the sync, and can still
  push out of order until every node runs it.
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
