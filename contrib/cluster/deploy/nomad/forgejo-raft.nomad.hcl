# forgejo-raft as ONE Nomad job: one task group per consensus slot, each
# pinned on its own machine.
#
#   nomad job run -var-file=example.vars.hcl forgejo-raft.nomad.hcl
#
# Why the job looks like this: Raft decides who leads and who holds the
# data, not the scheduler. Nomad must therefore never move a member, never
# start a replacement on another machine, and never start a node that the
# Recover witnesses have fenced. Membership changes go through the cluster
# (Site Administration > Cluster), not through `nomad job run`.
#
# What the job relies on outside of it, set up once:
#   - on each machine, a host volume "forgejo-<slot>" (client config) on an
#     encrypted, persistent disk. It holds the node's key and certificate
#     (tls/), the consensus data directory and the repositories. The key is
#     born there and never enters Nomad Variables or Vault;
#   - on each machine, a host volume "kitsunium-consensus" (client config)
#     on /run/kitsunium-consensus, a tmpfs created at boot owned by uid 1000
#     with mode 0700 (systemd-tmpfiles:
#     "d /run/kitsunium-consensus 0700 1000 1000 -"). It holds the
#     consensus node's boot-scoped STORAGE_FAILED marker, which must outlive
#     a restart of the allocation and go with a reboot of the machine
#     (ADR 0152 D4, R9-05);
#   - each Nomad client's meta forgejo_slot = "<slot>", read by the JOIN
#     drain check, and a client max_kill_timeout of at least kill_timeout;
#   - the Nomad ACL policy forgejo-raft.policy.hcl attached to this job;
#   - Vault (vault-setup.sh): the PKI mount pki-consensus with one role per
#     slot, and the JWT auth mount jwt-consensus with one role per slot;
#   - the Nomad Variable nomad/jobs/forgejo-raft with db_password and
#     secret_key (the job reads it with its own workload identity);
#   - a PostgreSQL that every node reaches, highly available (Patroni or a
#     managed service): the cluster replicates git refs, not SQL.

variable "image" {
  description = "The forgejo-raft image, pinned by digest"
  type        = string
}

variable "namespace" {
  type    = string
  default = "default"
}

variable "datacenters" {
  type    = list(string)
  default = ["dc1"]
}

variable "nodes" {
  description = "slot => the Nomad node id of the machine that hosts it (node.unique.id)"
  type        = map(string)
}

variable "addresses" {
  description = "slot => the machine's private address, which the other members reach (never a wildcard)"
  type        = map(string)
}

variable "root_url" {
  description = "The public URL users reach, through the load balancer in front of the nodes"
  type        = string
}

variable "db_host" {
  description = "PostgreSQL, host:port, always the primary (a proxy such as HAProxy or PgBouncer in front of it)"
  type        = string
}

variable "witnesses" {
  description = "The Recover witnesses' URLs, at least two, on hosts that fail independently of the nodes"
  type        = list(string)
}

variable "witness_keys" {
  description = "Their Ed25519 public keys, hex, in the same order"
  type        = list(string)
}

variable "vault_addr" {
  type = string
}

locals {
  # The [cluster] settings both tasks render into app.ini. The witness check
  # reads the same file the server reads, so they must not diverge.
  cluster_env = {
    FORGEJO__cluster__ENABLED            = "true"
    FORGEJO__cluster__WITNESSES          = join(",", var.witnesses)
    FORGEJO__cluster__WITNESS_KEYS       = join(",", var.witness_keys)
    FORGEJO__cluster__TLS_CERT_FILE      = "/data/tls/node.pem"
    FORGEJO__cluster__TLS_KEY_FILE       = "/data/tls/node.key"
    FORGEJO__cluster__TLS_CA_FILE        = "/data/tls/ca.pem"
    FORGEJO__security__DISABLE_GIT_HOOKS = "true"
  }
}

job "forgejo-raft" {
  namespace   = var.namespace
  datacenters = var.datacenters
  type        = "service"

  dynamic "group" {
    for_each = var.nodes
    labels   = [group.key]

    content {
      count = 1

      # Pinned on its machine: the slot IS the machine, and its key lives on
      # that machine's disk. A datacenter or a class is not an identity.
      constraint {
        attribute = "${node.unique.id}"
        value     = group.value
      }

      # Nomad never moves a member by itself: a member started elsewhere
      # would have no data and no key. Raft tolerates one node down; an
      # operator replaces a dead machine through the cluster.
      reschedule {
        attempts  = 0
        unlimited = false
      }

      # A client that loses its servers keeps its allocation running.
      disconnect {
        replace = false
      }

      # Local restarts only, then the allocation stays failed. A fenced
      # node (exit 77) is an operator's matter, not a retry loop, and so is
      # a consensus node that stopped itself (exit 65, 69, 73, 74 or 76):
      # Nomad cannot skip a restart on an exit code, but its stop marker,
      # in the data directory or in /run/kitsunium-consensus, refuses every
      # restart until an operator clears it. After a STORAGE_FAILED, reboot
      # the machine, never only the allocation.
      restart {
        attempts = 3
        interval = "30m"
        delay    = "1m"
        mode     = "fail"
      }

      # One node at a time, and only once the previous one is healthy: the
      # health check below is ready only when the node follows a leader.
      update {
        max_parallel      = 1
        health_check      = "checks"
        min_healthy_time  = "30s"
        healthy_deadline  = "10m"
        progress_deadline = "15m"
        auto_revert       = false
      }

      network {
        mode = "host"
        port "http" {
          static = 3000
        }
      }

      volume "data" {
        type   = "host"
        source = "forgejo-${group.key}"
      }

      volume "consensus-run" {
        type   = "host"
        source = "kitsunium-consensus"
      }

      # The Recover fence, before Forgejo. It asks every witness whether the
      # cluster this data directory belongs to was superseded by a Recover.
      # It exits 77 when one says so: the main task then never starts, and
      # reschedule (attempts = 0) keeps it down. While none answers it waits
      # and asks again, so the node starts once the witnesses are back; it
      # exits 78 on wrong witness settings.
      # It runs through the image's setup so that app.ini carries the same
      # FORGEJO__* settings as the server's.
      task "witness-check" {
        driver = "docker"

        lifecycle {
          hook    = "prestart"
          sidecar = false
        }

        config {
          image        = var.image
          network_mode = "host"
          entrypoint   = ["/bin/bash", "-c"]
          args         = ["cd /etc/s6/gitea && source ./setup >/dev/null && exec su-exec git /usr/local/bin/gitea cluster witness check"]
        }

        volume_mount {
          volume      = "data"
          destination = "/data"
        }

        env = merge(local.cluster_env, {
          FORGEJO__cluster__SLOT        = group.key
          FORGEJO__cluster__LISTEN_ADDR = "${var.addresses[group.key]}:7480"
          # The operator's one-boot override, FORGEJO_CLUSTER_WITNESS_OVERRIDE,
          # is never part of the job. Its SHA-256 goes in
          # FORGEJO__cluster__WITNESS_OVERRIDE_SHA256 when you decide to keep
          # one; its use is logged.
        })

        resources {
          cpu    = 100
          memory = 128
        }
      }

      # A block label cannot be interpolated: the task is a dynamic block so
      # that its name is forgejo-<slot>, which the Vault role binds.
      dynamic "task" {
        for_each = [group.key]
        labels   = ["forgejo-${group.key}"]

        content {
          driver       = "docker"
          kill_timeout = "90s"
          # Leave the load balancer before the stop hands leadership over.
          shutdown_delay = "5s"

          config {
            image        = var.image
            network_mode = "host"
          }

          volume_mount {
            volume      = "data"
            destination = "/data"
          }

          volume_mount {
            volume      = "consensus-run"
            destination = "/run/kitsunium-consensus"
          }

          # The default identity: the task API socket (secrets/api.sock) and
          # the Nomad Variables of this job. The JOIN drain check reads the
          # Nomad nodes with it, under forgejo-raft.policy.hcl.
          identity {
            file = true
            env  = false
          }

          # The workload identity the certificate renewer logs in to Vault
          # with, at auth/jwt-consensus/login under the slot's role. Nomad
          # rotates the file; the renewer reads it again at each login.
          identity {
            name        = "consensus"
            aud         = ["jwt-consensus"]
            ttl         = "1h"
            file        = true
            change_mode = "noop"
          }

          # Secrets from the Nomad Variable nomad/jobs/forgejo-raft.
          template {
            destination = "secrets/forgejo.env"
            env         = true
            change_mode = "restart"
            data        = <<-EOT
              {{ with nomadVar "nomad/jobs/forgejo-raft" -}}
              FORGEJO__database__PASSWD={{ .db_password }}
              FORGEJO__security__SECRET_KEY={{ .secret_key }}
              {{- end }}
            EOT
          }

          env = merge(local.cluster_env, {
            USER_UID = "1000"
            USER_GID = "1000"

            FORGEJO__server__ROOT_URL       = var.root_url
            FORGEJO__server__HTTP_PORT      = "3000"
            FORGEJO__database__DB_TYPE      = "postgres"
            FORGEJO__database__HOST         = var.db_host
            FORGEJO__database__NAME         = "forgejo"
            FORGEJO__database__USER         = "forgejo"
            FORGEJO__database__SSL_MODE     = "require" # verify-full once the database CA is in the image
            FORGEJO__security__INSTALL_LOCK = "true"
            FORGEJO__session__PROVIDER      = "db"
            FORGEJO__metrics__ENABLED       = "true"

            FORGEJO__cluster__SLOT               = group.key
            FORGEJO__cluster__LISTEN_ADDR        = "${var.addresses[group.key]}:7480"
            FORGEJO__cluster__OBJECT_LISTEN_ADDR = "${var.addresses[group.key]}:7481"

            # Certificate renewal in process, through Vault, with the
            # workload identity above. The CSR is made from the key on disk.
            FORGEJO__cluster__RENEW_VAULT_ADDR       = var.vault_addr
            FORGEJO__cluster__RENEW_VAULT_MOUNT      = "pki-consensus"
            FORGEJO__cluster__RENEW_VAULT_ROLE       = group.key
            FORGEJO__cluster__RENEW_VAULT_JWT_FILE   = "/secrets/nomad_consensus.jwt"
            FORGEJO__cluster__RENEW_VAULT_AUTH_MOUNT = "jwt-consensus"
            FORGEJO__cluster__RENEW_VAULT_AUTH_ROLE  = group.key

            # The JOIN drain check: a node joining by itself is refused
            # while its Nomad node drains, is ineligible or not ready.
            FORGEJO__cluster__NOMAD_ADDR       = "unix:///secrets/api.sock"
            FORGEJO__cluster__NOMAD_TOKEN_FILE = "/secrets/nomad_token"
            FORGEJO__cluster__NOMAD_SLOT_META  = "forgejo_slot"
          })

          service {
            name     = "forgejo-raft"
            provider = "nomad"
            port     = "http"
            address  = var.addresses[group.key]
            tags     = ["slot-${group.key}"]

            # Ready means: the node follows a known leader and its git
            # repositories have caught up with the replicated ref table
            # once since it started. A learner is never ready.
            check {
              type     = "http"
              path     = "/api/healthz"
              interval = "10s"
              timeout  = "3s"
            }
          }

          resources {
            cpu    = 2000
            memory = 2048
          }
        }
      }
    }
  }
}
