# The lab's Forgejo job for `make nomad-e2e`: the deployment example's
# shape (contrib/cluster/deploy/nomad) on the one dev node, with
# raw_exec and the static Forgejo binary instead of the image. Two groups:
#   - node-a, whose data directory is clear: the witness prestart passes and
#     the main task renews a copy of node-a's certificate through Vault, logged
#     in with its workload identity;
#   - node-c, whose data directory belongs to a lineage both witnesses
#     recorded as superseded: the prestart exits 77 and the main task never
#     starts.
# Nomad's agent runs as root in the container: Forgejo is told it may (lab
# only).

variable "node_id" { type = string }
variable "cluster_id" { type = string }
variable "witnesses" { type = string }
variable "witness_keys" { type = string }
variable "vault_addr" { type = string }

locals {
  slots = { node-a = "clear", node-c = "fenced" }
}

job "forgejo" {
  datacenters = ["*"]
  type        = "service"

  meta {
    rollout = "script"
  }

  dynamic "group" {
    for_each = local.slots
    labels   = [group.key]

    content {
      constraint {
        attribute = "${node.unique.id}"
        value     = var.node_id
      }

      reschedule {
        attempts  = 0
        unlimited = false
      }

      disconnect {
        replace = false
      }

      restart {
        attempts = 0
        mode     = "fail"
      }

      task "witness-check" {
        driver = "raw_exec"

        lifecycle {
          hook    = "prestart"
          sidecar = false
        }

        config {
          command = "/usr/local/bin/forgejo"
          args    = ["--config", "${NOMAD_TASK_DIR}/app.ini", "--work-path", "${NOMAD_TASK_DIR}/w", "cluster", "witness", "check"]
        }

        env {
          GITEA_I_AM_BEING_UNSAFE_RUNNING_AS_ROOT = "true"
        }

        template {
          destination = "local/app.ini"
          data        = <<-EOT
            WORK_PATH = {{ env "NOMAD_TASK_DIR" }}/w
            [cluster]
            ENABLED = true
            CLUSTER_ID = ${var.cluster_id}
            DATA_DIR = /lab/nomad/${group.key}/consensus
            SLOT = ${group.key}
            LISTEN_ADDR = 127.0.0.1:7480
            OBJECT_LISTEN_ADDR = 127.0.0.1:7481
            TLS_CERT_FILE = /lab/nomad/${group.key}/tls/node.pem
            TLS_KEY_FILE = /lab/nomad/${group.key}/tls/node.key
            TLS_CA_FILE = /lab/nomad/${group.key}/tls/ca.pem
            HOOK_SOCKET = /tmp/forgejo-lab-${group.key}.sock
            WITNESSES = ${var.witnesses}
            WITNESS_KEYS = ${var.witness_keys}
          EOT
        }

        resources {
          cpu    = 100
          memory = 128
        }
      }

      # A block label cannot be interpolated: the task is a dynamic block
      # so that its name is the slot's, forgejo-<slot> (R9-13).
      dynamic "task" {
        for_each = [group.key]
        labels   = ["forgejo-${group.key}"]

        content {
          driver = "raw_exec"

          identity {
            name        = "consensus"
            aud         = ["jwt-consensus"]
            ttl         = "1h"
            file        = true
            change_mode = "noop"
          }

          config {
            command = "/bin/sh"
            args    = ["${NOMAD_TASK_DIR}/run.sh"]
          }

          env {
            GITEA_I_AM_BEING_UNSAFE_RUNNING_AS_ROOT = "true"
          }

          template {
            destination = "local/app.ini"
            data        = <<-EOT
              WORK_PATH = {{ env "NOMAD_TASK_DIR" }}/w
              [cluster]
              ENABLED = true
              CLUSTER_ID = ${var.cluster_id}
              DATA_DIR = /lab/nomad/${group.key}/consensus
              SLOT = ${group.key}
              LISTEN_ADDR = 127.0.0.1:7480
              OBJECT_LISTEN_ADDR = 127.0.0.1:7481
              TLS_CERT_FILE = {{ env "NOMAD_TASK_DIR" }}/tls/node.pem
              TLS_KEY_FILE = {{ env "NOMAD_TASK_DIR" }}/tls/node.key
              TLS_CA_FILE = {{ env "NOMAD_TASK_DIR" }}/tls/ca.pem
              HOOK_SOCKET = /tmp/forgejo-lab-${group.key}.sock
              RENEW_VAULT_ADDR = ${var.vault_addr}
              RENEW_VAULT_JWT_FILE = {{ env "NOMAD_SECRETS_DIR" }}/nomad_consensus.jwt
              RENEW_VAULT_AUTH_MOUNT = jwt-consensus
              RENEW_VAULT_AUTH_ROLE = ${group.key}
              RENEW_TTL = 24h
            EOT
          }

          # Renews a copy of the slot's certificate (the live node keeps its
          # own), then idles. The JWT is copied out for the script's refusal
          # check under another slot's role.
          template {
            destination = "local/run.sh"
            data        = <<-EOT
              set -u
              mkdir -p "$NOMAD_TASK_DIR/tls"
              cp /lab/${group.key}/tls/node.pem /lab/${group.key}/tls/node.key /lab/${group.key}/tls/ca.pem "$NOMAD_TASK_DIR/tls/"
              cp "$NOMAD_SECRETS_DIR/nomad_consensus.jwt" /lab/nomad/${group.key}.jwt
              if out="$(/usr/local/bin/forgejo --config "$NOMAD_TASK_DIR/app.ini" --work-path "$NOMAD_TASK_DIR/w" cluster cert renew 2>&1)"; then
                echo "RENEW-OK $(echo "$out" | tail -n 1)"
              else
                echo "RENEW-FAILED $(echo "$out" | tail -n 3 | tr '\n' ' ')"
              fi
              exec sleep 3600
            EOT
          }

          resources {
            cpu    = 100
            memory = 128
          }
        }
      }
    }
  }
}
