#!/bin/sh
# Starts, inside a member's container, the sshd that serves the observers'
# export on port 2222: keys only, the git user only, no terminal, agent or
# forward, the forced command in authorized_keys (written by
# scripts/observer-e2e.sh), its own host key.
set -eu
d=/data/observer-sshd
install -d -m 0700 -o git -g git "$d"
[ -s "$d/host_ed25519" ] || ssh-keygen -q -t ed25519 -N '' -f "$d/host_ed25519"
touch "$d/authorized_keys"
chown git:git "$d/authorized_keys"
chmod 0600 "$d/authorized_keys"
cat >"$d/sshd_config" <<CONF
Port 2222
HostKey $d/host_ed25519
AuthorizedKeysFile $d/authorized_keys
AllowUsers git
PasswordAuthentication no
KbdInteractiveAuthentication no
PermitRootLogin no
AllowAgentForwarding no
AllowTcpForwarding no
X11Forwarding no
PermitTTY no
PidFile $d/sshd.pid
CONF
exec /usr/sbin/sshd -f "$d/sshd_config"
