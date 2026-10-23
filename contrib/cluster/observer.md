# Observer nodes

An observer is a forgejo-raft node set by hand with `[cluster] ROLE =
observer`. It keeps a copy of the forge that survives the loss, or the total
compromise, of every member: refs, git objects, LFS objects, attachments,
packages and Forgejo's PostgreSQL, rebuildable at any of its dated
checkpoints with `forgejo cluster recover --from-observer`.

It is never a member. It never votes, never joins, never appears in the
membership, and plays no part in a boot, an election or a fence: the voters
run the cluster alone, and nothing on them waits for an observer. It opens
no port. It starts every connection itself, over SSH, to a forced read-only
command the members serve, and it holds no PostgreSQL credential.

## How a checkpoint is made

Every `CHECKPOINT_INTERVAL`, the leader takes a checkpoint, a causal cut:

1. Its own writers queue behind a short fence, and the log consumer, which
   writes the SQL of each push, is held at the last applied entry.
2. A barrier entry commits at index B. Its Apply captures the ref table at
   B, on the leader.
3. The consumer is let through to B and no further, and drained: the SQL of
   every entry up to B is committed.
4. Under that fence, the leader reads PostgreSQL's flushed WAL position L,
   its timeline and system identifier, and creates a restore point named
   after the checkpoint.
5. The fence is released, the stored files listed (outside the fence, so a
   large storage never freezes the writers; uploads still in a storage's
   temporary directory are left out), the WAL segment switched so L is
   archived at once, the artifacts written (the table at B, the list of
   stored files), and a position entry carrying L and the artifacts'
   digests is committed.

A checkpoint is then (lineage, B, refs root, L, timeline, system id). The SQL
committed after step 4 is past L, whatever entry it derives from. The pushes
on other members are ordered after B by the log, and their SQL is held by the
consumer. What a web request writes to SQL beside a push (a pull request
marked merged after its merge commit, for example) is not fenced; it is
reconciled after a recovery (`forgejo doctor check --all --fix`, `forgejo
cluster reconcile --apply`).

A position read in one term is refused in another: if leadership changed
during the checkpoint, another dispatcher may have written SQL past B. A
position that takes more than 5 seconds to commit after L was read is
discarded, and the next period takes another.

## On the members

### PostgreSQL

The observer supports one configuration: PostgreSQL dedicated to Forgejo.
The export refuses a PostgreSQL cluster that holds any database besides
Forgejo's, `postgres`, `template0` and `template1`, and any tablespace
besides `pg_default` and `pg_global`: the base backup is one tar on stdout.

On every PostgreSQL node:

```ini
archive_mode = on            # not "always": only the primary archives, and a
                             # promoted node archives its new timeline
archive_command = 'forgejo cluster wal-archive --dir /srv/forgejo/wal-archive %p %f'
archive_timeout = 30s
synchronous_commit = on      # a commit acknowledged is flushed, so it is before L
```

`forgejo cluster wal-archive` is idempotent, as PostgreSQL requires: a file
already archived with the same bytes is success, other bytes under the same
name are refused, and the archive is never overwritten. Timeline histories
and backup history files are archived too, and so is the `.partial` segment
a promoted standby archives for the old timeline: PostgreSQL retries a
refused file forever and archives nothing after it. The export never serves
a `.partial` file. Where the PostgreSQL host has no
Forgejo binary, `contrib/cluster/observer/archive-wal.sh` does the same in
POSIX shell. The archive directory must be readable by the Forgejo node's
export: on the same host, or on a shared volume. Keep at least a week of it,
and alert when `pg_stat_archiver.failed_count` grows: a gap forces the
observer to take a new base backup.

The checkpoint role and the base backup role:

```sql
CREATE ROLE forgejo_checkpoint NOLOGIN;
GRANT EXECUTE ON FUNCTION pg_create_restore_point(text) TO forgejo_checkpoint;
GRANT EXECUTE ON FUNCTION pg_switch_wal() TO forgejo_checkpoint;
GRANT EXECUTE ON FUNCTION pg_control_system() TO forgejo_checkpoint;
GRANT forgejo_checkpoint TO forgejo;   -- Forgejo's own user SET ROLEs to it

CREATE ROLE forgejo_basebackup LOGIN REPLICATION PASSWORD '...';
```

and in `pg_hba.conf`, the replication role from the Forgejo nodes only.

### Forgejo

```ini
[cluster]
CHECKPOINT_INTERVAL = 1m
CHECKPOINT_ROLE = forgejo_checkpoint
WAL_ARCHIVE_DIR = /srv/forgejo/wal-archive
PG_BASEBACKUP = /usr/bin/pg_basebackup
PG_BASEBACKUP_DSN = host=10.0.0.10 port=5432 user=forgejo_basebackup passfile=/run/forgejo/pgpass
OBSERVER_RECIPIENTS = nas-paris=<64 hex>,nas-perigueux=<64 hex>
```

`CHECKPOINT_INTERVAL` is 0, off, by default. The checkpoints are two new
command kinds in the log, which a member of an older version stops on: turn
it on only once every member runs this version. It needs PostgreSQL, and the
LFS, attachment and package storages local: an observer cannot copy an
object store, and the node refuses to start the coordinator otherwise.

`OBSERVER_RECIPIENTS` are the observers' X25519 public keys. The export
seals the database's `SECRET_KEY` to the observer its SSH key belongs to;
every other secret is rotated by a recovery, never restored.

### SSH

The export is served by OpenSSH, in a dedicated account on each member,
with one line per observer in its `authorized_keys`:

```
restrict,command="forgejo cluster export --observer nas-paris",from="203.0.113.7" ssh-ed25519 AAAA...
```

`restrict` disables the terminal, agent, X11 and port forwarding; the
command reads one request on stdin, forwards it to the running node's
private socket if it is one of the read-only export ops, and answers on
stdout. It refuses a session that has a terminal or an agent, which a line
without `restrict` would allow. Forgejo's built-in SSH server never runs it:
it serves git only.

The export ops: the checkpoints and the committed log come from the leader
only, after a commit confirmed by a quorum; a member that does not lead
answers `NOT_LEADER` with the leader's slot and its own term. The
checkpoint's artifacts come from the leader that wrote them. Packs (bounded
to ref values), WAL files, stored files and base backups come from any
member, and the observer checks them by digest against what the leader told
it.

## On the observer

```ini
[cluster]
ROLE = observer
OBSERVER_NAME = nas-paris
OBSERVER_DIR = /volume1/observer
OBSERVER_ENDPOINTS = node-a=forgejo-export@10.0.0.1:22,node-b=forgejo-export@10.0.0.2:22,node-c=forgejo-export@10.0.0.3:22
OBSERVER_KNOWN_HOSTS = /run/observer/known_hosts
OBSERVER_SSH_KEY_FILE = /run/observer/id_ed25519
OBSERVER_SECRETS_KEY_FILE = /run/observer/secrets.key
OBSERVER_SNAPSHOT_HOOK = /usr/local/bin/observer-snapshot
OBSERVER_POLL_INTERVAL = 15s
OBSERVER_STAGING_QUOTA = 10737418240
```

`ROLE = observer` refuses every key that would make the node a member:
`LISTEN_ADDR`, `OBJECT_LISTEN_ADDR`, `OBJECT_PORT`, `OBJECT_PEERS`, `SLOT`,
`PEERS`, `PEER_PINS`, `EXPECTED_NODES`, `CLUSTER_ID`, `JOIN_GRANT_TTL`,
`OPERATORS`, `ENABLED` and `CHECKPOINT_INTERVAL`. `forgejo web` refuses to
start on it. The node runs `forgejo cluster observe`, a long-lived process.

`forgejo cluster observe keygen --name nas-paris --out /run/observer` makes
the SSH key and the secrets key and prints the `authorized_keys` line and
the `OBSERVER_RECIPIENTS` entry for the members. Keep both keys in an
encrypted volume. The host keys of the members go in the pinned
`known_hosts`: an unknown key is refused, never learnt.

### What a pass does

Each pass asks every pinned endpoint and keeps the view of the highest term:
a deposed leader that still answers is passed over, and a hint is never
followed off the pinned endpoints. Then:

- It follows the committed log where the engine reads it back, checking that
  each batch continues what it accepted: consecutive indexes, terms that
  never decrease nor pass the leader's.
- It takes the newest checkpoint past the newest it accepted, into
  `staging/`, and verifies it: the table at B by the digest the position
  carries and the root the barrier recorded; the packs of every live ref
  through `git index-pack --strict`, and their whole closure; the stored files
  by digest (an LFS object by its id); the secrets bundle, opened with the
  observer's key and kept sealed; and PostgreSQL, covered by a base backup
  that starts before L and contiguous WAL from its start to L on the
  timeline path to the checkpoint's timeline, histories included. A position
  past the point where its timeline was abandoned in a failover is refused:
  that tail was lost. Without coverage, it takes a base backup
  (`pg_basebackup -D - -Ft -X none`), and the next checkpoint is covered.
- Its refs are created under `refs/observer/<checkpoint>/` of each
  repository, in one transaction of create-only updates: an accepted
  checkpoint's ref is never moved, and `gc.auto = 0` keeps every object.
  Deletions are tombstones in the manifest. The checkpoint is then published
  by one rename into `checkpoints/`: VERIFIED.
- The snapshot hook runs. It must snapshot the whole observer directory in
  one atomic snapshot, print the snapshot's identity on its first line and
  exit 0; it is given the checkpoint's name, and must be idempotent for it.
  Its success is recorded in `checkpoints/<name>/PROTECTED`: the checkpoint
  is PROTECTED. While the hook fails, ingestion and pruning stop, and an
  alert is raised.

Nothing accepted is rewritten. The base backups and WAL files are stored as
files, never booted on ingest; a base backup holding a tablespace, a link, a
device or an escaping name is refused.

### Quarantine

A fork or a rollback is never taken as history. Each of these stops
ingestion until a human acts, the accepted checkpoints untouched:

- the leader's log does not hold, at the observer's cursor, the term it
  accepted (a fork);
- the leader confirms a commit behind one already accepted (a rollback);
- an accepted checkpoint is described otherwise (a fork);
- the membership names the observer;
- the leader serves another lineage: a Recover or a restore started a new
  history.

`forgejo cluster observe status` says why. `forgejo cluster observe accept
<lineage>` ends the quarantine: naming the new lineage, the observer follows
it; naming the current one after a fork, it restarts its log from its newest
checkpoint. Beside the running service, which holds the directory, the
command leaves the request in the directory (`ACCEPT`) and the service takes
it at its next pass; `observe status` then shows the quarantine gone, or why
the accept was refused. No restart is needed. One process at a time holds an
observer directory (`observer.lock`): `observe once` beside the service is
refused. The first lineage the observer meets is its trust root; check it
against a member's `forgejo cluster status` (`lineage`).

### Integrity is not authenticity

The checksums prove the transfer, not the intent. A member compromised
whole serves a perfectly valid malicious history (a force-push, a `DELETE`),
and the observer accepts it as a new checkpoint: it cannot tell. The defence
is that every earlier checkpoint stays as it was, PROTECTED by a snapshot the
cluster cannot reach, and that a human chooses the recovery point.

### Retention, freshness, alerts

The versions are the hook's snapshots, and their retention (14 days, 8
weeks, 12 months, 3 years) is the snapshot system's, out of the observer's
reach: the ingestion process cannot delete a version. In the live
directory, a protected checkpoint older than 72 hours is let go of (never the
newest protected one, never while the hook fails), and with it the base
backups, WAL and stored files no remaining checkpoint needs; its snapshots
keep it. Staging is bounded by `OBSERVER_STAGING_QUOTA`: at the quota,
ingestion stops; nothing protected is deleted.

`STATUS.json` and `ALERTS` in the observer directory are for the host's
monitoring: freshness is the age of the newest PROTECTED checkpoint, never of
a verified one; alerts cover a quarantine, a failing hook, no protected
checkpoint, a freshness over 15 minutes, the staging quota and free space
under twice the quota. The budget from a commit to the observer's protected
watermark is the checkpoint period plus the poll interval plus the transfer:
with a 30 s period and a 15 s poll, about a minute; the lab measures it.

## Recovering from an observer

A human act, on a new node, every member of the old cluster off:

1. Choose the checkpoint: `forgejo cluster observe list --dir <snapshot>`
   lists them, newest first, with their age, their state and whether they
   restore from that directory. In its own snapshot a checkpoint reads
   VERIFIED, not PROTECTED: the hook takes the snapshot, and the PROTECTED
   record is written once it exists. Mount the chosen snapshot read-only.
2. Make the observers' new keys (`observe keygen`): every key the old
   cluster saw is rotated.
3. Fence the old cluster on both witnesses, as for `recover`: `forgejo
   cluster witness supersede --old <old id> --new <new id> --voters ...` on
   each witness host, for every voter the checkpoint names (its manifest
   lists them, `voters`), then `forgejo cluster witness receipt` for each
   voter from each witness. An old member that comes back stops at its
   boot.
4. On the new node, Forgejo stopped, its certificate issued for a new cluster
   id:

```sh
forgejo cluster recover --from-observer <checkpoint> --new-cluster <new id> \
  --observer-dir /mnt/snapshot --secrets-key /run/observer/secrets.key \
  --new-observer-recipient nas-paris=<new hex> --out /srv/recover \
  --pgdata /srv/pg/data [--pg-ctl /usr/lib/postgresql/18/bin/pg_ctl] \
  --witness-key <hex> --witness-key <hex> \
  --fenced <voter id>:stopped:<receipt 1>+<receipt 2> ... \
  [--old-peer node-a=10.0.0.1:7480 ...]
```

It follows this order, and stops for the operator where a step is theirs;
running the same command again resumes from `<out>/progress.json`. A step
cut midway (SSH lost, kill, power loss, an error) is replayed: the
PostgreSQL step empties again `--pgdata` and the WAL restore directory,
which it found empty and began writing; the repository step keeps every
repository already written whole, writes again one it left marked
`forgejo-restore-incomplete`, and refuses any other non-empty directory;
the stored files already copied with the same bytes are kept:

1. Forgejo stays fenced and offline: the command refuses while the node
   answers on its socket. The old cluster is fenced and silent: both
   witnesses' receipts for every voter the checkpoint names, and no old
   member answering at its consensus address (`PEERS` and `--old-peer`;
   `--force-old-members-answer` overrides that probe, logged). A checkpoint
   taken before the voters were recorded names none: at least one
   `--fenced` is then required, and the operator answers for the others.
2. Rotation first: `<out>/rotated.ini` holds the checkpoint's `SECRET_KEY`
   (the database's encrypted columns need it), new JWT secrets and the
   observers' new key. The command stops until `app.ini` carries them.
3. PostgreSQL: the base is extracted without the source's configuration
   (`postgresql.conf`, `postgresql.auto.conf`, includes, `conf.d`, signals,
   `tablespace_map`), and replayed with the observer's own:
   `restore_command` limited to the checkpoint's WAL, `recovery_target_lsn =
   L`, `recovery_target_timeline` the checkpoint's (never `latest`),
   `recovery_target_inclusive = off`, `recovery_target_action = promote`,
   `hot_standby = off`. Without `--pg-ctl`, the command stops for the
   operator to start PostgreSQL as its owner. It checks the system
   identifier and the promotion past the checkpoint's timeline: PostgreSQL
   promotes only once the target is reached.
4. The repositories from the checkpoint's namespace (fetched with
   `fetch.fsckObjects`), and the stored files by digest.
5. A fresh archive configuration for the recovered primary.
6. Verification: the refs against the SQL branch rows, every LFS object
   present, every 2FA secret opening with the restored `SECRET_KEY`; then
   every access token, OAuth2 grant, session and runner token is revoked.
7. The schema is migrated to this binary's first, so the sequences of the
   tables a migration creates are moved too. Then the identifiers move past
   the checkpoint, once: every sequence of the
   database by 1,000,000,000 (`setval`; a sequence whose type leaves less
   room moves half of what it has left, one at its end stops the recovery),
   and every repository's numbering (`issue_index`, for issues and pull
   requests, and `action_run_index`) by 100,000, a repository without a
   counter getting one past its highest number. Mails, webhooks, runners and
   other forges hold the IDs and numbers handed out after the checkpoint;
   they are never handed out again. Each move is printed; the recovery is
   recorded in the `recovery-advance` cursor, so a resumed run moves nothing
   twice.
8. Only then the new cluster, this node its only voter.
9. The loss, stated: everything after the checkpoint, and the replicated
   history before it.

Then start Forgejo, run `forgejo cluster import` to put the restored refs in
the new table, `forgejo doctor check --all --fix` and `forgejo cluster
reconcile --apply`, add the other members, and give the observers their new
key and the new lineage (`observe accept`).

## Limits of this version

- The kitsunium engine does not read its log back yet (its `ReadCommitted`
  is a draft in the SDK): with it, the observer follows checkpoints alone,
  and the log is not kept between them. A checkpoint is complete on its own:
  the table at B is captured at the barrier, not replayed from a log.
- A recovery starts a new lineage through `init`, `bootstrap` and `import`:
  the replicated audit and grants of the old cluster are not carried (the
  SDK draft `ImportObserved` will keep the lineage chain).
- The SQL a web request writes beside a push is reconciled after a
  recovery, not fenced.
