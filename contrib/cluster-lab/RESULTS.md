# Lab results

Outputs of real runs from scratch, copied from the scripts. Each run starts
from `make reset`: the volumes, the PKI and the cluster are new. Slots are
`node-a`, `node-b` and `node-c` (forgejo-1 to forgejo-3); `node-d` is the
Nomad agent's node in `make nomad-e2e`.

## Latest run: SDK aa07aca4 with pinned JOINs (15/10/2026)

Forgejo `dbc55348a5`, kitsunium/sdk `aa07aca4`. One phase at a time, the lab
stopped at the end:

| Phase | rc | Time |
|---|---|---|
| `make reset` | 0 | 7s |
| `make build` | 0 | 336s |
| `make up` | 0 | 135s |
| `make bootstrap` | 0 | 23s |
| `make witnesses` | 0 | 13s |
| `make nomad-e2e` | 0 | 51s |
| `make e2e` | 0 | 36s |
| `make chaos-e2e` | 0 | 172s |
| `docker compose stop` | 0 | |

### `make nomad-e2e`

```text
== 1. Vault: pki-consensus and the jwt-consensus login, one role per slot
OK    pki role node-a: kitsunium-consensus://577745f1567e1bdc42f050557d113812/slot/node-a
OK    pki role node-b: kitsunium-consensus://577745f1567e1bdc42f050557d113812/slot/node-b
OK    pki role node-c: kitsunium-consensus://577745f1567e1bdc42f050557d113812/slot/node-c
OK    jwt-consensus role node-a: job forgejo, task forgejo-node-a, policy pki-consensus-node-a
OK    jwt-consensus role node-b: job forgejo, task forgejo-node-b, policy pki-consensus-node-b
OK    jwt-consensus role node-c: job forgejo, task forgejo-node-c, policy pki-consensus-node-c
== 2. A clear data directory for node-a, a superseded one for node-c
OK    node-a: node 626770b270897df9 of cluster adb4eec65276e8cb073d2ea83edc3de2
OK    node-c: node b289757ea5e7e6af of cluster 67b3f88688ba6cddff0ae3f04a499874
OK    witness-a records 67b3f88688ba6cddff0ae3f04a499874 superseded by 5bb2a5d4d534e508c9823fe9c6144a2a
OK    witness-b records 67b3f88688ba6cddff0ae3f04a499874 superseded by 5bb2a5d4d534e508c9823fe9c6144a2a
== 3. The job: node-a passes its prestart and renews through its workload identity
OK    node-a allocated (0s)
OK    node-a's main task forgejo-node-a running (1s)
OK    the renewal reported (3s)
      RENEW-OK renewed: serial 303693302580781129578993038485630540978193580426, valid until 2026-10-16T14:10:21Z
OK    witness-check of node-a exited 0
OK    Vault login with node-a's workload identity, certificate signed under pki-consensus/sign/node-a
      claims: {"aud":"jwt-consensus","nomad_namespace":"default","nomad_job_id":"forgejo","nomad_task":"forgejo-node-a"}
OK    node-a's identity refused under node-b's role (HTTP 400)
OK    node-a's Vault token cannot sign for node-b (HTTP 403)
== 4. node-c: the witness prestart exits 77, Forgejo never starts, no reschedule
OK    node-c's allocation failed (0s)
OK    witness-check exited 77
      fenced: this cluster was superseded by a Recover: the node is fenced (by 5bb2a5d4d534e508c9823fe9c6144a2a, says http://10.89.0.51:7482)
OK    forgejo-node-c never started
OK    Nomad did not reschedule node-c (reschedule attempts = 0)
OK    node-a still running
== 5. The JOIN drain check against this Nomad (slot node-d, its node's meta)
      sdk_test.go:376: JOIN of node-d refused, the joiner reads: JOIN_REFUSED: the Nomad node of slot node-d is ineligible
      --- PASS: TestSelfJoinRefusedWhileTheNomadNodeDrains (0.85s)
OK    granted JOIN refused: the Nomad node is ineligible
      sdk_test.go:382: JOIN of node-d admitted as a learner
      --- PASS: TestSelfJoinRefusedWhileTheNomadNodeDrains (0.34s)
OK    granted JOIN admitted as a learner: the node is eligible
OK    the sleeper runs (1s)
OK    the node drains (0s)
      sdk_test.go:376: JOIN of node-d refused, the joiner reads: JOIN_REFUSED: the Nomad node of slot node-d is draining
      --- PASS: TestSelfJoinRefusedWhileTheNomadNodeDrains (0.83s)
OK    granted JOIN refused: the Nomad node is draining
```

### `make e2e`

```text
== 0. The lab is up
OK    forgejo-1 healthy (0s)
OK    forgejo-2 healthy (0s)
OK    forgejo-3 healthy (0s)
== 1. User and repository through node 1
OK    repository lab-admin/e2e-1792073455 created on node 1 (auto_init false)
OK    node 2 sees the repository (0s)
OK    node 3 sees the repository (0s)
OK    first push to node 1 at 71581801e0d9
OK    node 2 serves main at 71581801e0d9 (0s)
OK    node 3 serves main at 71581801e0d9 (0s)
== 2. Push to node 2, read on node 3 at once
OK    node 3 serves e2e-push-node2 at 6d4573805d11 right after the push
== 3. Stop the Raft leader, push again
OK    Raft leader is forgejo-3
OK    forgejo-3 killed (SIGKILL)
OK    push to forgejo-1 accepted at 892749585958, 5.580324000s after the leader stopped, first try
OK    node 2 serves e2e-after-leader at 892749585958 (0s)
== 4. Stop the Patroni primary, Forgejo recovers
OK    pg-2 (Patroni leader) stopped
OK    a new Patroni leader (0s)
OK    forgejo-1 answers the API again (1s)
OK    push after the PostgreSQL failover at b36b856c2b20
OK    node 2 serves e2e-after-pg at b36b856c2b20 (0s)
== 5. Restore, the restored nodes catch up
OK    forgejo-3 healthy (7s)
OK    node 3 serves e2e-after-leader at 892749585958 (0s)
OK    node 3 serves e2e-after-pg at b36b856c2b20 (0s)
OK    pg-2 streaming again (0s)
== E2E PASSED (lab-admin/e2e-1792073455)
```

### `make chaos-e2e`

```text
== Setup
OK    lab-admin/chaos-1792073491 main at 1b05ce0b855f
== 1. Partition a follower
OK    leader forgejo-1, partitioning follower forgejo-2
OK    forgejo-2 cut off the network
OK    push to forgejo-1 accepted at 0b751e2d75a0
OK    forgejo-3 serves p1 (0s)
OK    forgejo-2 reconnected
OK    forgejo-2 caught up on p1 (0s)
== 2. Partition the leader
OK    partitioning leader forgejo-1
OK    forgejo-1 cut off the network
OK    the majority elects a new leader (1s)
OK    push to forgejo-2 accepted at b045597402c5
OK    forgejo-3 serves p2 (0s)
OK    the isolated forgejo-1 refuses a write alone
OK    forgejo-1 reconnected
OK    forgejo-1 caught up on p2 (12s)
OK    the refused write never appeared
== 3. Stop a follower's container
OK    forgejo-1 stopped
OK    push to forgejo-3 accepted at 4c7a3006e5b5
OK    forgejo-1 healthy (3s)
OK    forgejo-1 caught up on p3 (0s)
== CHAOS PASSED (lab-admin/chaos-1792073491)
```

## Recover and Vault renewal (15/10/2026)

Forgejo `eab00d6847`, kitsunium/sdk `9e97695c`, the same slot names. These
two scenarios do not depend on the JOIN; they were not repeated after the
SDK bump.

### `make recover-e2e` (after `make reset build up bootstrap witnesses`)

```text
== 1. A repository and a branch on the three nodes
OK    node 1 serves main (0s)
OK    node 2 serves main (0s)
OK    node 3 serves main (0s)
OK    cluster 08a7c28e4a27fc54966db138ba78890f: node-a=9db59e8bd706a982 node-b=6b82efc58b0f359d node-c=ae99ee756197ff9e
== 2. The majority is lost; node-a stops for Recover
OK    new cluster id 9e00046c38c606cdd9ace34bfe31b329
== 3. The fence on both witnesses, the receipts, Recover
OK    witness-a recorded 08a7c28e4a27fc54966db138ba78890f superseded by 9e00046c38c606cdd9ace34bfe31b329
OK    witness-b recorded 08a7c28e4a27fc54966db138ba78890f superseded by 9e00046c38c606cdd9ace34bfe31b329
OK    receipts for 6b82efc58b0f359d from both witnesses
OK    receipts for ae99ee756197ff9e from both witnesses
OK    recover refuses receipts from one witness only
== 4. node-a serves, the old branch and a new push
OK    node-a ready (3s)
OK    node-a serves main at f5500d837d30
OK    node-a takes a push alone
== 5. The old pair restarts on its old data: refused, exit 77
OK    forgejo-2 refused by the witnesses, exit 77 (1s)
OK    forgejo-3 refused by the witnesses, exit 77 (0s)
OK    forgejo-2 never ready
OK    forgejo-3 never ready
OK    the recovered cluster holds node-a alone
== 6. The operator's override, logged
OK    the override is logged (1s)
== 7. Without the override, refused again
OK    forgejo-2 refused again (1s)
```

### `make vault-renew-e2e` (after `make reset up bootstrap`)

```text
== 1. Vault, the pki-consensus mount and one role per slot
OK    pki role node-a: kitsunium-consensus://88c3ae50ac9cfdb26ec9773c6c23fe0e/slot/node-a
OK    pki role node-b: kitsunium-consensus://88c3ae50ac9cfdb26ec9773c6c23fe0e/slot/node-b
OK    pki role node-c: kitsunium-consensus://88c3ae50ac9cfdb26ec9773c6c23fe0e/slot/node-c
== 2. The nodes with the renewer
== 3. cluster cert renew on each node
renewed: serial 488061160755248937149919115522651453616306574059, valid until 2026-11-12T18:14:26Z
OK    forgejo-1 renewed (serial=557D6B5B4F14A2E59A8FC53DE027D4975E60D2EB), same SPKI c19c92f4d49d49e2, SAN unchanged
renewed: serial 325497548195714125257434518726851162696520586653, valid until 2026-11-12T18:14:28Z
OK    forgejo-2 renewed (serial=3903D09AC6C3552C174D6A3A88F40448C384559D), same SPKI 8d5f7f0e749ee728, SAN unchanged
renewed: serial 690952374446543732330836424535228988008459818233, valid until 2026-11-12T18:14:29Z
OK    forgejo-3 renewed (serial=790760441F760DA89DD47B8ED5CCAFF6C50E54F9), same SPKI 954d3963bdb5608f, SAN unchanged
== 4. Vault refuses another slot's URI SAN
OK    node-a's role refuses node-b's SAN (HTTP 400)
== 5. Each node restarted in turn on its renewed certificate
OK    forgejo-1 ready again (2s)
OK    forgejo-2 ready again (2s)
OK    forgejo-3 ready again (2s)
slot node-a  role leader  term 5  leader 11a055b284d19a0d  commit 5  applied 5
slot node-b  role follower  term 5  leader 11a055b284d19a0d  commit 5  applied 5
slot node-c  role follower  term 5  leader 11a055b284d19a0d  commit 5  applied 5
```

## Notes

- `make chaos-e2e` needs the `lab-admin` account that `make e2e` creates: run
  it after `make e2e`.
- Before, a `chaos-e2e` run failed once when Docker's embedded DNS stopped
  resolving `haproxy` after a network reconnect (consensus had caught up, all
  three nodes at the same commit index), and a second run lost the Docker
  daemon itself. Both were the host's Docker, not the cluster; the run above
  passed with Docker's VM at 8 GB.
- Memory measured on an earlier run: a little over 1.2 GB for the whole lab.
