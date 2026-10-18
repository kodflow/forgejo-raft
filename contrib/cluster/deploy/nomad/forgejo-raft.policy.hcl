# Nomad ACL policy of the forgejo-raft job's workload identity. Attach it
# to the job (and to nothing else):
#
#   nomad acl policy apply -namespace default -job forgejo-raft \
#     -description "forgejo-raft: JOIN drain check" forgejo-raft forgejo-raft.policy.hcl
#
# What it grants, read-only:
#   - node:read, so that before a node joins the cluster by itself, Forgejo
#     reads GET /v1/nodes and GET /v1/node/<id> and refuses the join while
#     the machine hosting that slot is draining, ineligible or not ready;
#   - read-job in the job's namespace, so that a task can read its own job
#     and allocations (status, debugging). Nomad scopes capabilities by
#     namespace, not by job: run forgejo-raft in a namespace of its own to
#     make this "its own job only". Nothing writes.
# The job's own Nomad Variables (nomad/jobs/forgejo-raft) need no policy:
# a workload identity reads them implicitly.

node {
  policy = "read"
}

namespace "default" {
  capabilities = ["read-job"]
}
