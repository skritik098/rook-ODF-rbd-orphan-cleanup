#!/bin/bash
# ============================================================================
# capture_rbd_data.sh
# ============================================================================
# Run this on a system with access to the Ceph cluster and Kubernetes API.
# It produces 3 files inside a capture directory that can be fed into
# rbd_tree_builder_live.py to build the parent-child-snapshot tree.
#
# Usage:
#   bash capture_rbd_data.sh [POOL_NAME] [OUTPUT_DIR]
#
# Defaults:
#   POOL_NAME  = ocs-storagecluster-cephblockpool
#   OUTPUT_DIR = rbd_capture
#
# Toolbox note:
#   On ODF clusters, prefix Ceph commands with the toolbox wrapper, e.g.:
#     CEPH_CMD_PREFIX="oc rsh -n openshift-storage $(oc get pod -n openshift-storage -l app=rook-ceph-tools -o name)"
#   Or run directly inside the toolbox pod.
# ============================================================================

set -euo pipefail

POOL="${1:-ocs-storagecluster-cephblockpool}"
OUTDIR="${2:-rbd_capture}"

mkdir -p "$OUTDIR"

echo "[1/3] Capturing trash list for pool: $POOL"
# ============================================================================
# COMMAND 1 — Trash list
# ============================================================================
rbd trash ls "$POOL" --format json > "$OUTDIR/trash_list.json" 2>/dev/null || echo "[]" > "$OUTDIR/trash_list.json"
echo "      -> $(jq length "$OUTDIR/trash_list.json") image(s) in trash"


echo "[2/3] Capturing image info + snapshots for pool: $POOL"
# ============================================================================
# COMMAND 2 — Image info + snap info (normal pool images AND trash images)
#
# Produces a delimited text file:  images_and_snaps.txt
# Each image block is wrapped with markers for easy parsing.
# ============================================================================
(
  # --- 2a: Loop over normal (active) images in the pool ---
  for img in $(rbd ls "$POOL" 2>/dev/null); do
    echo "---IMAGE_START---"
    echo "name=${img}"
    echo "source=pool"
    echo "pool=${POOL}"
    echo "---INFO---"
    rbd info "${POOL}/${img}" 2>/dev/null || echo "(info unavailable)"
    echo "---SNAPS---"
    rbd snap ls "${POOL}/${img}" --format json 2>/dev/null || echo "[]"
    echo "---IMAGE_END---"
  done

  # --- 2b: Loop over trashed images (by image-id) ---
  for entry in $(jq -c '.[]' "$OUTDIR/trash_list.json" 2>/dev/null); do
    tid=$(echo "$entry"  | jq -r '.id')
    tname=$(echo "$entry" | jq -r '.name')
    echo "---IMAGE_START---"
    echo "name=${tname}"
    echo "source=trash"
    echo "trash_id=${tid}"
    echo "pool=${POOL}"
    echo "---INFO---"
    rbd info "${POOL}" --image-id "${tid}" 2>/dev/null || echo "(info unavailable)"
    echo "---SNAPS---"
    rbd snap ls "${POOL}" --image-id "${tid}" --format json 2>/dev/null || echo "[]"
    echo "---IMAGE_END---"
  done
) > "$OUTDIR/images_and_snaps.txt"

total=$(grep -c "^---IMAGE_START---$" "$OUTDIR/images_and_snaps.txt" || echo 0)
echo "      -> ${total} image(s) captured"


echo "[3/3] Capturing PersistentVolume details"
# ============================================================================
# COMMAND 3 — PV list (all PVs in JSON)
# ============================================================================
kubectl get pv -o json > "$OUTDIR/pv_list.json" 2>/dev/null || echo '{"items":[]}' > "$OUTDIR/pv_list.json"
pv_count=$(jq '.items | length' "$OUTDIR/pv_list.json")
echo "      -> ${pv_count} PV(s) captured"


echo ""
echo "Capture complete. Files in: $OUTDIR/"
echo "  trash_list.json        — rbd trash listing"
echo "  images_and_snaps.txt   — image info + snapshots (pool + trash)"
echo "  pv_list.json           — Kubernetes PV details"
echo ""
echo "Next step:"
echo "  python3 rbd_tree_builder_live.py $OUTDIR/ [--output tree.json]"