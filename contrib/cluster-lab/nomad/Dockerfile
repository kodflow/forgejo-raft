# A Nomad dev agent that can run Forgejo's offline subcommands with
# raw_exec: the static Forgejo binary of the lab image is copied beside
# Nomad. Built by `make build` once the Forgejo image exists.
ARG FORGEJO_IMAGE=forgejo-cluster-lab/forgejo:raft-cluster
ARG NOMAD_IMAGE=hashicorp/nomad:2.0.7@sha256:e3f31dad30ec4ae6ab2f3799cea40a87d12bb4be88bbb75234f63f4f90c6c52d
FROM ${FORGEJO_IMAGE} AS forgejo
FROM ${NOMAD_IMAGE}
COPY --from=forgejo /app/gitea/gitea /usr/local/bin/forgejo
COPY lab.hcl /etc/nomad.d/lab.hcl
COPY entrypoint.sh /etc/nomad.d/entrypoint.sh
