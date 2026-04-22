#!/bin/bash
# ============================================================================
# capture_rbd_data.sh
# ============================================================================
# Captures RBD image data from a live ODF cluster for offline tree building.
#
# Usage:
#   bash capture_rbd_data.sh <TOOLBOX_POD> [POOL] [OUTDIR]
#
# Examples:
#   bash capture_rbd_data.sh rook-ceph-tools-77d6988b97-vvrjd
#   bash capture_rbd_data.sh rook-ceph-tools-77d6988b97-vvrjd ocs-storagecluster-cephblockpool rbd_capture
#
#   # Auto-detect toolbox pod:
#   bash capture_rbd_data.sh $(oc get pod -n openshift-storage -l app=rook-ceph-tools -o jsonpath='{.items[0].metadata.name}')
#
# Output (4 files):
#   rbd_capture/
#     trash_list.json       — raw: rbd trash ls --format json
#     all_images.txt        — raw: rbd info + snap ls per image (marker-delimited)
#     pv_list.json          — raw: oc get pv -o json
#     vsc_list.json         — raw: oc get volumesnapshotcontent -o json
#
# Next step:
#   python3 rbd_tree_builder_live.py rbd_capture/ [--output tree.json]
# ============================================================================

set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 <TOOLBOX_POD> [POOL] [OUTDIR]"
  echo ""
  echo "Example:"
  echo "  $0 rook-ceph-tools-77d6988b97-vvrjd"
  exit 1
fi

TOOLBOX="$1"
POOL="${2:-ocs-storagecluster-cephblockpool}"
OUTDIR="${3:-rbd_capture}"
NAMESPACE="${NAMESPACE:-openshift-storage}"

mkdir -p "$OUTDIR"

echo "======================================================================"
echo "RBD Data Capture"
echo "  Toolbox : $TOOLBOX"
echo "  Pool    : $POOL"
echo "  Output  : $OUTDIR/"
echo "======================================================================"

# ----------------------------------------------------------------------------
# STEP 1 — Trash list
# ----------------------------------------------------------------------------
echo ""
echo "[1/3] Capturing rbd trash list ..."

oc -n "$NAMESPACE" exec "$TOOLBOX" -- rbd trash ls "$POOL" --format json \
  > "$OUTDIR/trash_list.json" 2>/dev/null \
  || echo "[]" > "$OUTDIR/trash_list.json"

echo "      -> done"

# ----------------------------------------------------------------------------
# STEP 2 — All image info + snapshots into ONE file
#
# Format (plain text markers, raw JSON bodies):
#
#   ### IMAGE <name> SOURCE pool INFO
#   { ... rbd info --format json ... }
#   ### IMAGE <name> SNAPS
#   [ ... rbd snap ls --all --format json ... ]
#
# IMPORTANT: --all flag on snap ls ensures ALL snapshots are returned,
# including protected snapshots used as clone parents.
# Without --all, rbd snap ls may omit these and break the tree.
# ----------------------------------------------------------------------------
echo ""
echo "[2/3] Capturing image info + snapshots ..."

> "$OUTDIR/all_images.txt"

count=0

# --- Active pool images ---
for img in $(oc -n "$NAMESPACE" exec "$TOOLBOX" -- rbd ls "$POOL" 2>/dev/null); do
  echo "### IMAGE $img SOURCE pool INFO" >> "$OUTDIR/all_images.txt"
  oc -n "$NAMESPACE" exec "$TOOLBOX" -- rbd info "$POOL/$img" --format json \
    >> "$OUTDIR/all_images.txt" 2>/dev/null || echo '{}' >> "$OUTDIR/all_images.txt"

  echo "" >> "$OUTDIR/all_images.txt"
  echo "### IMAGE $img SNAPS" >> "$OUTDIR/all_images.txt"
  oc -n "$NAMESPACE" exec "$TOOLBOX" -- rbd snap ls "$POOL/$img" --all --format json \
    >> "$OUTDIR/all_images.txt" 2>/dev/null || echo '[]' >> "$OUTDIR/all_images.txt"

  echo "" >> "$OUTDIR/all_images.txt"
  count=$((count + 1))
  echo "      [$count] $img"
done

# --- Trashed images ---
if command -v jq &>/dev/null; then
  # Read trash entries into an array to avoid subshell counter issue
  trash_entries=$(jq -r '.[] | "\(.id) \(.name)"' "$OUTDIR/trash_list.json" 2>/dev/null || true)

  while read -r tid tname; do
    [ -z "$tid" ] && continue

    echo "### IMAGE $tname SOURCE trash TRASH_ID $tid INFO" >> "$OUTDIR/all_images.txt"
    oc -n "$NAMESPACE" exec "$TOOLBOX" -- rbd info "$POOL" --image-id "$tid" --format json \
      >> "$OUTDIR/all_images.txt" 2>/dev/null || echo '{}' >> "$OUTDIR/all_images.txt"

    echo "" >> "$OUTDIR/all_images.txt"
    echo "### IMAGE $tname SNAPS" >> "$OUTDIR/all_images.txt"
    oc -n "$NAMESPACE" exec "$TOOLBOX" -- rbd snap ls "$POOL" --image-id "$tid" --all --format json \
      >> "$OUTDIR/all_images.txt" 2>/dev/null || echo '[]' >> "$OUTDIR/all_images.txt"

    echo "" >> "$OUTDIR/all_images.txt"
    count=$((count + 1))
    echo "      [$count] $tname (trash)"
  done <<< "$trash_entries"
else
  echo "      [warn] jq not found — skipping trash images. Install jq to include them."
fi

echo "      -> ${count} image(s) captured total"

# ----------------------------------------------------------------------------
# STEP 3 — Kubernetes PVs + VolumeSnapshotContents
# ----------------------------------------------------------------------------
echo ""
echo "[3/3] Capturing Kubernetes resources ..."

oc get pv -o json > "$OUTDIR/pv_list.json" 2>/dev/null \
  || echo '{"items":[]}' > "$OUTDIR/pv_list.json"
echo "      -> PVs captured"

oc get volumesnapshotcontent -o json > "$OUTDIR/vsc_list.json" 2>/dev/null \
  || echo '{"items":[]}' > "$OUTDIR/vsc_list.json"
echo "      -> VolumeSnapshotContents captured"

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
echo ""
echo "======================================================================"
echo "Capture complete. Files:"
echo "  $OUTDIR/trash_list.json"
echo "  $OUTDIR/all_images.txt   (${count} images)"
echo "  $OUTDIR/pv_list.json"
echo "  $OUTDIR/vsc_list.json"
echo ""
echo "Next step:"
echo "  python3 rbd_tree_builder_live.py $OUTDIR/ [--output tree.json]"
echo "======================================================================"