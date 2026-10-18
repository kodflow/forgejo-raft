# Forgejo cluster lab

A disposable lab, under Docker Compose, of the active/active Forgejo cluster.
The operator's reference (the `[cluster]` keys, init, bootstrap, joining,
recovery, metrics) is `contrib/cluster/README.md`; the tutorial is the
repository's README.

## What it holds

- Three Consul servers (`consul-1` to `consul-3`), in Raft.
- Three PostgreSQL 18 under Patroni (`pg-1` to `pg-3`), Consul as the DCS,
  synchronous replication in strict mode (required: contrib/cluster/README.md).
  Each PostgreSQL talks to the Consul of its own slot.
- HAProxy in front of them: port 5000 always leads to the primary, checked
  through `/primary` on Patroni's REST API (port 8008).
- A lab PKI (`pki`, run once). The cluster id is 16 bytes (32 hex digits).
  The CA stays in the `pki` volume, which no Forgejo mounts. Each slot's key
  (`node-a`, `node-b`, `node-c`) is born in its node's volume and never leaves
  it. Each certificate carries a single URI SAN
  `kitsunium-consensus://<cluster>/slot/<slot>`; the SHA-256 of each SPKI is in
  `/pki/spki.txt`. Run again, the PKI keeps the keys.
- Three Forgejo (`forgejo-1` to `forgejo-3`, slots `node-a` to `node-c`), on
  ports 3001 to 3003. They share the database; each keeps its repositories
  and its consensus log in its own volume. The `forgejo` role is capped at 60
  connections.
- `make bootstrap` (`scripts/bootstrap.sh`), run by hand. It stops the three
  Forgejo, runs `cluster init` on each, then `cluster bootstrap` with the same
  member list, each in a one-off container of the same service (same volume,
  same address), and starts the servers again. A node not yet founded stops at
  start-up: that is expected before `make bootstrap`.
- Two Recover witnesses (`witness-a`, `witness-b`), a Vault dev server and a
  Nomad dev agent, for the scenarios below.

The image `forgejo-cluster-lab/forgejo:raft-cluster` is the upstream image,
built from the Forgejo tree with `--target forgejo`. Configuration goes
through `FORGEJO__section__KEY` variables. `[cluster]` requires
`DISABLE_GIT_HOOKS = true`.

`CLUSTER_ENABLED=false make up` starts the Forgejo nodes without the cluster.

## Memory

Caps: Consul 192 MB, PostgreSQL 384 MB, HAProxy 64 MB, Forgejo 512 MB, about
3.4 GB at most for the whole lab. Measured: a little over 1.2 GB.

## Running it

From this directory:

```sh
make up-db       # Consul, Patroni and HAProxy only
make failover    # a Patroni failover behind HAProxy
make up          # the whole lab, Forgejo included
make bootstrap   # found the Forgejo cluster
make witnesses   # the two Recover witnesses and the boot fence
make e2e         # the end-to-end run
make chaos-e2e   # partitions and restarts, checked
make status
make reset       # stop everything and wipe the volumes
```

The Forgejo tree is taken next to this repository by default
(`FORGEJO_SRC`). Its `go.mod` pins a published SDK. To try an unpublished SDK,
`SDK_SRC` joins it through a `go.work`: `make build` then writes
`.build/Dockerfile.forgejo` (the tree's Dockerfile plus two lines).

```sh
FORGEJO_SRC=/path/to/forgejo SDK_SRC=/path/to/sdk make build
make build             # without go.work, the SDK of go.mod
```

## Failures

```sh
make chaos SCENARIO="forgejo 2"     # stop one Forgejo
make chaos SCENARIO="pg-primary"    # stop the Patroni primary
make chaos SCENARIO="partition 3"   # cut one Forgejo off the network
make heal                           # put everything back
```

`make chaos-e2e` chains three failures and checks each. A follower cut off:
pushes go through, it catches up on its return. The leader cut off: the
majority elects a leader and takes pushes, the old leader refuses to write
alone, then catches up. A follower stopped: it catches up on restart.

Addresses are fixed (10.89.0.0/24): a node cut off comes back on the same one.

## The end-to-end run

`scripts/e2e.sh` stops at the first failure, with a non-zero code.

1. A user and a repository, created through node 1.
2. An HTTP push to node 2; the branch is read at once on node 3. The
   repository is created empty, then a first push goes through node 1
   (`AUTO_INIT=1` has Forgejo write the first commit).
3. The Raft leader is killed (SIGKILL). A push follows at once, in one try,
   and must pass within 15 s (`MAX_PUSH_AFTER_LEADER_STOP`); another node
   serves it.
4. The Patroni primary is stopped. Forgejo recovers, a push goes through.
5. The stopped nodes come back and catch up with the missing branches.

## The Recover witnesses

`witness-a` and `witness-b` play the two witness hosts. Each runs
`forgejo cluster witness serve` and signs its answers with its Ed25519 key.
The operator writes a superseded lineage only inside the container, with
`forgejo cluster witness supersede`, never over the network.

```sh
make witnesses     # start the witnesses, write .env, recreate the nodes with the boot check
make recover-e2e   # lose the majority, Recover with both witnesses, the old pair refused
```

`make recover-e2e` is destructive: it leaves the recovered cluster on
`node-a` alone, `node-b` and `node-c` refused. Start again with
`make reset up bootstrap witnesses`. The run checks, in order:

1. a repository and a branch served by the three nodes;
2. `node-b` and `node-c` stopped, then `node-a` stopped for the Recover;
3. the lineage recorded on both witnesses, one signed receipt per voter and
   per witness, a Recover refused with one witness's receipts and accepted
   with both, `node-a`'s certificate re-issued on the same key for the new id
   (`pki/recert.sh`);
4. `node-a` ready, its branch from before the loss served, a new push
   accepted;
5. `node-b` and `node-c` restarted on their old data, refused at start-up
   (`exit 77`), never ready;
6. the operator's override (`FORGEJO_CLUSTER_WITNESS_OVERRIDE`) lets `node-b`
   start, and the log says so;
7. without the override, `node-b` is refused again.

Inside the container, s6 restarts the refused process in a loop. Under
Nomad, a prestart task runs `forgejo cluster witness check`, which exits 77.

## Certificate renewal through Vault

`make vault-renew-e2e` (after `make up bootstrap`) starts a Vault dev server
(`127.0.0.1:8200`, token `make secret NAME=VAULT_DEV_TOKEN`), mounts
`pki-consensus` on the lab's CA and sets one role per slot
(`require_cn=false`, the slot's single URI SAN, EC keys). Each node renews
with `forgejo cluster cert renew` from its own key. The script checks that the
serial changes, that the SPKI and the SAN stay, that `node-a`'s role refuses
`node-b`'s SAN, then restarts every node on its new certificate. The
in-process renewer reads `RENEW_VAULT_ADDR` and `RENEW_VAULT_TOKEN_FILE`, which
the script writes into `.env`.

## Forgejo under Nomad

`make nomad-e2e` (after `make up bootstrap witnesses`) runs against a Nomad
dev agent (`nomad`, `127.0.0.1:4646`). Its single node stands for a node
machine, with the meta `forgejo_slot = "node-d"`. The lab job,
`nomad/forgejo-lab.nomad.hcl`, has the shape of the deployment example
(`contrib/cluster/deploy/nomad`), with `raw_exec` and the static Forgejo
binary copied into the Nomad image. The script proves three things.

- **Vault through the workload identity.** The `jwt-consensus` mount reads
  Nomad's keys. Each role binds `nomad_namespace`, `nomad_job_id` and
  `nomad_task`, and its policy signs for its slot only. The task
  `forgejo-node-a` renews a copy of `node-a`'s certificate with
  `forgejo cluster cert renew`, logged in with its JWT. The same JWT is refused
  under `node-b`'s role, and its token cannot sign for `node-b`.
- **The witness check as a prestart task.** The `node-c` group starts from a
  data directory whose lineage both witnesses record as superseded. The
  `witness-check` task exits 77, `forgejo-node-c` never starts, the allocation
  fails and Nomad does not reschedule it.
- **A JOIN refused on a drained node.** The Go test
  `TestSelfJoinRefusedWhileTheNomadNodeDrains` reads this Nomad. A granted
  JOIN of slot `node-d` is refused while the node is ineligible, admitted as a
  learner once it is eligible again, then refused during a drain; the joiner
  reads the reason. A `sleeper` job that ignores the stop signal holds the
  drain open for the test.

The lab's cluster is not touched and the job is purged at the end. Nomad runs
in a privileged container, in the host's cgroup namespace, because its client
manages cgroups v2, and `raw_exec` runs tasks as root there. Its ACLs are
therefore on: at start the container bootstraps them with the management
token `NOMAD_TOKEN` drawn into `.env`, and any request without it is refused.
Port 4646 stays bound to `127.0.0.1`. For the CLI from the host,
`NOMAD_TOKEN=$(make secret NAME=NOMAD_TOKEN) nomad status`.

## Results

The outputs of the latest full runs from scratch are in
[RESULTS.md](RESULTS.md).

## Limits of the lab

- No Consul ACLs, no TLS between PostgreSQL and its clients: the lab proves
  failover, not security.
- No secret is written in the repository. At the first `make`,
  `scripts/secrets.sh` draws random values (`openssl rand`) into `.env`, which
  git ignores: the PostgreSQL and Forgejo database passwords, `SECRET_KEY`,
  `lab-admin`'s password, the Vault dev token, the witness override secret and
  the Nomad token. It never replaces one, and `make reset` keeps them. To read
  one, for example `lab-admin`'s password (the account `make e2e` creates):

  ```sh
  make secret NAME=LAB_ADMIN_PASSWORD
  ```

  Deleting `.env` changes every secret: run `make reset` then.
- A repository created before the cluster goes once through
  `forgejo cluster import`. The end-to-end run creates its own afterwards.
- `make e2e` reads the Raft leader from `forgejo cluster status --json`,
  field `Role`, on each node.

## The CI end-to-end suite

`.github/workflows/e2e.yml` runs the lab on GitHub's runners, on every pull
request into `forgejo`, every push to it, nightly and on demand. It lays
`compose.ci.yaml` over `compose.yaml`:

- the autonomous mode (`EXPECTED_NODES = 3` and `PEERS`): no `make
  bootstrap`, the three nodes found the cluster by themselves;
- the consensus and the object channel on their own internal network,
  `raft` (10.89.1.0/24): a partition cuts that network alone, so the node
  keeps its HTTP port and PostgreSQL, and what refuses its writes is the
  consensus;
- LFS, attachments, avatars and archives on one volume every node mounts
  (the cluster does not replicate non-git files);
- Forgejo's own SSH server (127.0.0.1:2221 to 2223) and a webhook receiver
  (`ci/receiver.py`, 10.89.0.85:8080).

Each job brings up its own lab and runs one suite of `scripts/ci/`, which
prints one `CHECK <name>: PASS|FAIL` line per checkpoint:

- `cluster.sh`: the formation (one leader, three voters, the same status and
  commit index on every node), then the leader killed, the old leader back
  as a follower, a follower killed, two of three killed (no write, then
  recovery), a follower and then the leader partitioned (the minority
  refuses, the majority accepts, no divergence after the heal), the lab's
  own `e2e.sh` (with a Patroni failover), and never two leaders in one term,
  read from `/metrics` throughout;
- `features.sh`: accounts, an organisation and a repository, push and clone
  across nodes, branches, tags, issues, comments, labels, a reviewed and
  merged pull request, webhooks delivered exactly once, LFS, archives, the
  login page and git over SSH, each write read back from another node.

- `observer.sh`: suite D, on the manual bootstrap and `compose.observer.yaml`.
  It runs `scripts/observer-e2e.sh`, turns each step into a checkpoint, and
  checks from `/proc/net/tcp` that the observer never listens.

To run a suite by hand on the lab:

```sh
export COMPOSE_FILE="$PWD/compose.yaml:$PWD/compose.ci.yaml"
docker compose up -d consul-1 consul-2 consul-3 pg-1 pg-2 pg-3 haproxy forgejo-1 forgejo-2 forgejo-3 receiver
scripts/ci/cluster.sh
```

## The observer scenario

`compose.observer.yaml` lays the observer over the lab
(`contrib/cluster/observer.md`): the PostgreSQL nodes archive their WAL into
one shared volume, the Forgejo nodes take a checkpoint every 30 seconds and
serve the export through their own sshd on port 2222, an `observer` service
pulls and protects the checkpoints, its snapshot hook copying its directory
into a `snapshots` volume, and `forgejo-r` is the node a recovery rebuilds,
with its own PostgreSQL run as git in the same container.

```sh
make up bootstrap   # the lab
make observer-build # the image with sshd, pg_basebackup, PostgreSQL 18, git-lfs
make observer-e2e   # scripts/observer-e2e.sh
```

`scripts/observer-e2e.sh` protects a clean checkpoint (a branch, an LFS
object, an issue, a TOTP 2FA user) and measures the time from the push to its
protection; poisons the cluster with a valid force-push and a SQL `DELETE`,
which the observer protects too; fails PostgreSQL over and checks the next
checkpoint on the new timeline; loses every member and every PostgreSQL;
then recovers the clean checkpoint from its snapshot on `forgejo-r`, in the
plan's order, compares the replay with `recovery_target_inclusive` off and
on, and checks the clean branch, the deleted issue, the LFS object, the 2FA
login and the revoked tokens.

It is written and has not been run yet: it needs about 4 GB beside the lab,
and runs only with the lab owner's agreement. The Alpine package names of
`observer/Dockerfile` are to be confirmed against the image's release.

