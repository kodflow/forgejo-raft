# Example values for forgejo-raft.nomad.hcl. Every value here is a
# placeholder: replace each one with your own.

image = "registry.example.com/forgejo-raft@sha256:0000000000000000000000000000000000000000000000000000000000000000"

datacenters = ["dc1"]

# `nomad node status -verbose` prints each node's id.
nodes = {
  node-a = "00000000-0000-0000-0000-00000000000a"
  node-b = "00000000-0000-0000-0000-00000000000b"
  node-c = "00000000-0000-0000-0000-00000000000c"
}

# Private addresses (a VPN or a private network): the consensus on :7480 and
# the object channel on :7481 must never face the internet.
addresses = {
  node-a = "192.0.2.11"
  node-b = "192.0.2.12"
  node-c = "192.0.2.13"
}

root_url = "https://git.example.com/"
db_host  = "postgres.example.internal:5432"

# Two witnesses on hosts that fail independently of the three nodes.
witnesses = ["http://192.0.2.21:7482", "http://192.0.2.22:7482"]
witness_keys = [
  "1111111111111111111111111111111111111111111111111111111111111111",
  "2222222222222222222222222222222222222222222222222222222222222222",
]

vault_addr = "https://vault.example.internal:8200"
