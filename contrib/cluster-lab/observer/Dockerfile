# The lab's Forgejo image with what the observer scenario needs beside it:
# sshd for the members' export, pg_basebackup 18 for the base backups,
# PostgreSQL 18 itself for the recovered node, git-lfs and python3 for the
# checks. Built by `make observer-build`, never by `make build`. The Alpine
# package names are not checked here: confirm them against the image's
# Alpine release before the first run.
FROM forgejo-cluster-lab/forgejo:raft-cluster
USER root
RUN apk add --no-cache openssh-server postgresql18 postgresql18-client git-lfs python3
COPY --chmod=0755 snapshot-hook.sh /usr/local/bin/observer-snapshot
COPY --chmod=0755 sshd-export.sh /usr/local/bin/sshd-export
COPY --chmod=0755 --from=cluster-observer archive-wal.sh /usr/local/bin/archive-wal
