#!/usr/bin/env bash
# ODF Orphan Resource Finder - READ ONLY
# Finds: orphan RBD images, CephFS subvolumes, RBD/CephFS snapshots, orphan VSC

TOOLS_POD=$(oc get pod -A -l app=rook-ceph-tools --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')
TOOLS_NS=$(oc get pod -A -l app=rook-ceph-tools --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.namespace}')
RESULT="$(pwd)/orphan_report.txt"

echo "Tools pod: $TOOLS_POD | NS: $TOOLS_NS"
echo "ODF Orphan Report - $(date)" > $RESULT
echo "" >> $RESULT

# ============================================================
# Ceph Cluster Status
# ============================================================
echo ""; echo "=== ceph status ===" | tee -a $RESULT
oc exec -n $TOOLS_NS $TOOLS_POD -- ceph status | tee -a $RESULT

echo ""; echo "=== ceph df ===" | tee -a $RESULT
oc exec -n $TOOLS_NS $TOOLS_POD -- ceph df | tee -a $RESULT

echo ""; echo "=== ceph osd dump | grep ratio ===" | tee -a $RESULT
oc exec -n $TOOLS_NS $TOOLS_POD -- ceph osd dump | grep ratio | tee -a $RESULT

echo ""; echo "=== ceph osd df tree ===" | tee -a $RESULT
oc exec -n $TOOLS_NS $TOOLS_POD -- ceph osd df tree | tee -a $RESULT

# ============================================================
# RBD Images - csi-vol-* vs PVs
# ============================================================
echo "" | tee -a $RESULT
echo "=== RBD IMAGES ===" | tee -a $RESULT
echo "" | tee -a $RESULT

PV_IMAGES=$(oc get pv | grep -i rbd | awk '{print $1}' | \
    xargs -I{} oc get pv {} -o jsonpath='{.spec.csi.volumeAttributes.imageName}{"\n"}' | sort)

POOLS=$(oc exec -n $TOOLS_NS $TOOLS_POD -- ceph osd pool ls detail -f json | \
    jq -r '.[] | select(.application_metadata | has("rbd")) | .pool_name')

RBD_ORPHAN_LIST=""
for pool in $POOLS; do
    IMAGES=$(oc exec -n $TOOLS_NS $TOOLS_POD -- rbd ls --pool $pool --format json | \
        jq -r '.[] | select(startswith("csi-vol-"))')
    for img in $IMAGES; do
        if echo "$PV_IMAGES" | grep -qx "$img"; then
            echo "[MAPPED] $pool/$img" | tee -a $RESULT
        else
            echo "[ORPHAN] $pool/$img  <-- no PV found" | tee -a $RESULT
            RBD_ORPHAN_LIST="$RBD_ORPHAN_LIST\n$pool/$img"
        fi
    done
done

# ============================================================
# CephFS Subvolumes - vs PVs
# ============================================================
echo "" | tee -a $RESULT
echo "=== CEPHFS SUBVOLUMES ===" | tee -a $RESULT
echo "" | tee -a $RESULT

PV_SUBVOLS=$(oc get pv | grep -i cephfs | awk '{print $1}' | \
    xargs -I{} oc get pv {} -o jsonpath='{.spec.csi.volumeAttributes.subvolumeName}{"\n"}' | sort)

FS_NAME=$(oc exec -n $TOOLS_NS $TOOLS_POD -- ceph fs ls --format json | jq -r '.[0].name')

SUBVOLS=$(oc exec -n $TOOLS_NS $TOOLS_POD -- \
    ceph fs subvolume ls "$FS_NAME" --group_name csi --format json | \
    jq -r '.[].name')

CEPHFS_ORPHAN_LIST=""
for subvol in $SUBVOLS; do
    if echo "$PV_SUBVOLS" | grep -qx "$subvol"; then
        echo "[MAPPED] csi/$subvol" | tee -a $RESULT
    else
        echo "[ORPHAN] csi/$subvol  <-- no PV found" | tee -a $RESULT
        CEPHFS_ORPHAN_LIST="$CEPHFS_ORPHAN_LIST\ncsi/$subvol"
    fi
done

# ============================================================
# RBD Snapshots - csi-snap-* vs VSC
# ============================================================
echo "" | tee -a $RESULT
echo "=== RBD SNAPSHOTS ===" | tee -a $RESULT
echo "" | tee -a $RESULT

VSC_IDS=$(oc get volumesnapshotcontent -o jsonpath='{range .items[*]}{.status.snapshotHandle}{"\n"}{end}' | \
    awk -F'-' '{print $(NF-4)"-"$(NF-3)"-"$(NF-2)"-"$(NF-1)"-"$NF}')

RBD_SNAP_ORPHAN_LIST=""
for pool in $POOLS; do
    SNAP_IMAGES=$(oc exec -n $TOOLS_NS $TOOLS_POD -- \
        rbd ls -p $pool --format json | jq -r '.[] | select(startswith("csi-snap-"))')
    for img in $SNAP_IMAGES; do
        IMG_ID=$(echo "$img" | sed 's/^csi-snap-//')
        if echo "$VSC_IDS" | grep -qx "$IMG_ID"; then
            echo "[MAPPED] $pool/$img" | tee -a $RESULT
        else
            echo "[ORPHAN] $pool/$img  <-- no VSC found" | tee -a $RESULT
            RBD_SNAP_ORPHAN_LIST="$RBD_SNAP_ORPHAN_LIST\n$pool/$img"
        fi
    done
done

# ============================================================
# CephFS Snapshots - vs VSC
# ============================================================
echo "" | tee -a $RESULT
echo "=== CEPHFS SNAPSHOTS ===" | tee -a $RESULT
echo "" | tee -a $RESULT

CEPHFS_SNAP_ORPHAN_LIST=""
for subvol in $SUBVOLS; do
    SNAPS=$(oc exec -n $TOOLS_NS $TOOLS_POD -- \
        ceph fs subvolume snapshot ls "$FS_NAME" "$subvol" csi --format json 2>/dev/null | \
        jq -r '.[].name' 2>/dev/null)
    for snap in $SNAPS; do
        SNAP_ID=$(echo "$snap" | sed 's/^csi-snap-//')
        if echo "$VSC_IDS" | grep -qx "$SNAP_ID"; then
            echo "[MAPPED] $subvol/$snap" | tee -a $RESULT
        else
            echo "[ORPHAN] $subvol/$snap  <-- no VSC found" | tee -a $RESULT
            CEPHFS_SNAP_ORPHAN_LIST="$CEPHFS_SNAP_ORPHAN_LIST\n$subvol/$snap"
        fi
    done
done

# ============================================================
# VolumeSnapshotContent - check if bound VS exists
# ============================================================
echo "" | tee -a $RESULT
echo "=== VOLUMESNAPSHOTCONTENT ===" | tee -a $RESULT
echo "" | tee -a $RESULT

VSC_REFS=$(oc get volumesnapshotcontent -o json | jq -r \
    '.items[] | .metadata.name + "|" + .spec.volumeSnapshotRef.name + "|" + .spec.volumeSnapshotRef.namespace')

VSC_ORPHAN_LIST=""
while IFS='|' read -r vsc_name vs_name vs_ns; do
    [ -z "$vsc_name" ] && continue
    EXISTS=$(oc get volumesnapshot "$vs_name" -n "$vs_ns" --ignore-not-found 2>/dev/null)
    if [ -z "$EXISTS" ]; then
        echo "[ORPHAN] VSC=$vsc_name  VS=$vs_name  NS=$vs_ns  <-- no VolumeSnapshot found" | tee -a $RESULT
        VSC_ORPHAN_LIST="$VSC_ORPHAN_LIST\n$vsc_name"
    else
        echo "[MAPPED] VSC=$vsc_name  VS=$vs_name  NS=$vs_ns" | tee -a $RESULT
    fi
done << VSCEOF
$VSC_REFS
VSCEOF

# ============================================================
# SUMMARY TABLE
# ============================================================

print_col() {
    local h1="$1" h2="$2" list1="$3" list2="$4"
    local sep="|" w=80 i=1

    printf "\n%-${w}s %s %-${w}s\n" "$h1" "$sep" "$h2" | tee -a $RESULT
    printf "%-${w}s %s %-${w}s\n" "$(printf '%.0s-' {1..80})" "$sep" "$(printf '%.0s-' {1..80})" | tee -a $RESULT

    local c1=0 c2=0
    [ -n "$list1" ] && c1=$(printf "%b" "$list1" | grep -cv '^$' || true)
    [ -n "$list2" ] && c2=$(printf "%b" "$list2" | grep -cv '^$' || true)
    c1=${c1:-0}; c2=${c2:-0}
    local max_lines=$c1
    [ "$c2" -gt "$max_lines" ] && max_lines=$c2
    [ "$max_lines" -eq 0 ] && max_lines=1

    while [ $i -le $max_lines ]; do
        local v1="" v2=""
        [ -n "$list1" ] && v1=$(printf "%b" "$list1" | grep -v '^$' | sed -n "${i}p")
        [ -n "$list2" ] && v2=$(printf "%b" "$list2" | grep -v '^$' | sed -n "${i}p")
        [ -z "$v1" ] && v1="-"
        [ -z "$v2" ] && v2="-"
        printf "%-${w}s %s %-${w}s\n" "$v1" "$sep" "$v2" | tee -a $RESULT
        i=$((i+1))
    done
}

echo "" | tee -a $RESULT
echo "============================================================" | tee -a $RESULT
echo "=== SUMMARY ===" | tee -a $RESULT
echo "============================================================" | tee -a $RESULT

print_col "Orphan RBD Images (no PV)" "Orphan CephFS Subvolumes (no PV)" "$RBD_ORPHAN_LIST" "$CEPHFS_ORPHAN_LIST"

print_col "Orphan RBD Snapshots (no VSC)" "Orphan CephFS Snapshots (no VSC)" "$RBD_SNAP_ORPHAN_LIST" "$CEPHFS_SNAP_ORPHAN_LIST"

print_col "Orphan VolumeSnapshotContent (no VS)" "" "$VSC_ORPHAN_LIST" ""

echo "" | tee -a $RESULT
echo "============================================================" | tee -a $RESULT
printf "%-45s %s\n" "Orphan RBD Images:"           "$(printf "%b" "$RBD_ORPHAN_LIST"        | grep -c . 2>/dev/null || echo 0)" | tee -a $RESULT
printf "%-45s %s\n" "Orphan CephFS Subvolumes:"    "$(printf "%b" "$CEPHFS_ORPHAN_LIST"     | grep -c . 2>/dev/null || echo 0)" | tee -a $RESULT
printf "%-45s %s\n" "Orphan RBD Snapshots:"        "$(printf "%b" "$RBD_SNAP_ORPHAN_LIST"   | grep -c . 2>/dev/null || echo 0)" | tee -a $RESULT
printf "%-45s %s\n" "Orphan CephFS Snapshots:"     "$(printf "%b" "$CEPHFS_SNAP_ORPHAN_LIST"| grep -c . 2>/dev/null || echo 0)" | tee -a $RESULT
printf "%-45s %s\n" "Orphan VSC:"                  "$(printf "%b" "$VSC_ORPHAN_LIST"        | grep -c . 2>/dev/null || echo 0)" | tee -a $RESULT
echo "============================================================" | tee -a $RESULT

echo "" | tee -a $RESULT
echo "Done. Results in $RESULT"
