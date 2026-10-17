# The lab's Nomad: one dev agent, server and client. Its node stands for
# the machine of slot "node-d", a fourth slot that joins the consensus in the
# JOIN drain check of `make nomad-e2e`.
client {
  # Docker Desktop's VM reports no usable CPU frequency to the
  # fingerprinter: the compute is stated.
  cpu_total_compute = 4000

  # At least the Forgejo task's kill_timeout (ADR 0152 R8-17), and room for
  # the sleeper that holds a drain open in `make nomad-e2e`.
  max_kill_timeout = "5m"

  meta {
    forgejo_slot = "node-d"
  }
}

# No anonymous access: every call carries the lab's management token, which
# entrypoint.sh bootstraps from .env.
acl {
  enabled = true
}

plugin "raw_exec" {
  config {
    enabled = true
  }
}
