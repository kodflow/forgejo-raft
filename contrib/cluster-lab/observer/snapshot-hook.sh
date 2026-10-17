#!/bin/sh
# The lab's snapshot hook (contrib/cluster/observer.md, the hook contract):
# copies the whole observer directory to /snapshots/<checkpoint> and prints
# that identity. A copy is not atomic, which the lab tolerates because the
# observer writes nothing while its hook runs; a deployment uses its
# filesystem's snapshot (btrfs on a Synology, ZFS) and that system's
# retention. Idempotent: a snapshot already taken is reported again.
set -eu
: "${OBSERVER_DIR:?}" "${OBSERVER_CHECKPOINT:?}"
dst="/snapshots/$OBSERVER_CHECKPOINT"
if [ ! -e "$dst/.complete" ]; then
  rm -rf "$dst.partial"
  cp -a "$OBSERVER_DIR" "$dst.partial"
  touch "$dst.partial/.complete"
  rm -rf "$dst"
  mv "$dst.partial" "$dst"
fi
echo "lab-snapshot:$OBSERVER_CHECKPOINT"
