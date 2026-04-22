#!/usr/bin/env python3
"""
rbd_tree_builder_live.py
========================
Build Ceph RBD parent-child-snapshot relationship tree from data captured by
capture_rbd_data.sh on a live ODF cluster.

Reads:
  <capture_dir>/trash_list.json      — rbd trash ls --format json
  <capture_dir>/all_images.txt       — marker-delimited rbd info + snap ls
  <capture_dir>/pv_list.json         — oc get pv -o json
  <capture_dir>/vsc_list.json        — oc get volumesnapshotcontent -o json

Usage:
    python3 rbd_tree_builder_live.py <capture_dir> [--output output.json]
"""

import os
import sys
import re
import json
import argparse
from collections import defaultdict


# ---------------------------------------------------------------------------
# Trash list
# ---------------------------------------------------------------------------

def parse_trash_list(filepath):
    if not os.path.exists(filepath):
        return {}
    with open(filepath, "r", errors="replace") as fh:
        try:
            data = json.load(fh)
        except json.JSONDecodeError:
            return {}
    return {item["name"]: item["id"] for item in data if item.get("name")}


# ---------------------------------------------------------------------------
# all_images.txt parser
# ---------------------------------------------------------------------------

MARKER_RE = re.compile(
    r"^###\s+IMAGE\s+(\S+)\s+"
    r"(?:SOURCE\s+(\S+)\s+)?(?:TRASH_ID\s+(\S+)\s+)?"
    r"(INFO|SNAPS)\s*$"
)


def parse_all_images(filepath):
    images = {}
    snapshots = {}

    if not os.path.exists(filepath):
        return images, snapshots

    with open(filepath, "r", errors="replace") as fh:
        content = fh.read()

    lines = content.splitlines()
    sections = []

    current_header = None
    body_lines = []

    for line in lines:
        m = MARKER_RE.match(line)
        if m:
            if current_header is not None:
                sections.append((*current_header, "\n".join(body_lines)))
                body_lines = []
            current_header = (m.group(1), m.group(2), m.group(3), m.group(4))
        else:
            body_lines.append(line)

    if current_header is not None:
        sections.append((*current_header, "\n".join(body_lines)))

    for image_name, source, trash_id, section_type, body in sections:
        body = body.strip()

        if section_type == "INFO":
            raw = _safe_json_load(body, {})

            img = {
                "imageName": raw.get("name", image_name),
                "imageId": raw.get("id", ""),
                "pool": raw.get("pool", ""),
                "namespace": "",
            }

            if not img["imageId"] and trash_id:
                img["imageId"] = trash_id

            parent = raw.get("parent")
            if parent and isinstance(parent, dict):
                img["parent_pool"] = parent.get("pool", "")
                img["parent_image"] = parent.get("image", "")
                img["parent_snap"] = parent.get("snapshot", None)

            images[image_name] = img

        elif section_type == "SNAPS":
            snap_list = _safe_json_load(body, [])
            if snap_list:
                snapshots[image_name] = snap_list

    return images, snapshots


def _safe_json_load(text, default):
    text = text.strip()
    if not text:
        return default
    for i, ch in enumerate(text):
        if ch in ('{', '['):
            try:
                return json.loads(text[i:])
            except json.JSONDecodeError:
                return _safe_json_balanced(text, i, default)
    return default


def _safe_json_balanced(text, start, default):
    open_ch = text[start]
    close_ch = '}' if open_ch == '{' else ']'
    depth = 0
    for i in range(start, len(text)):
        if text[i] == open_ch:
            depth += 1
        elif text[i] == close_ch:
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[start:i + 1])
                except json.JSONDecodeError:
                    return default
    return default


# ---------------------------------------------------------------------------
# PV list
# ---------------------------------------------------------------------------

def parse_pv_json(filepath):
    pv_by_image = {}
    all_rbd_pvs = []

    if not os.path.exists(filepath):
        return pv_by_image, all_rbd_pvs

    with open(filepath, "r", errors="replace") as fh:
        try:
            data = json.load(fh)
        except json.JSONDecodeError:
            return pv_by_image, all_rbd_pvs

    for pv in data.get("items", []):
        if not isinstance(pv, dict):
            continue

        spec = pv.get("spec", {}) or {}
        csi = spec.get("csi", {}) or {}
        driver = csi.get("driver", "") or ""

        if "rbd" not in driver.lower():
            continue

        vol_attrs = csi.get("volumeAttributes", {}) or {}
        image_name = vol_attrs.get("imageName", "")
        if not image_name:
            continue

        pv_name = (pv.get("metadata", {}) or {}).get("name", "")
        claim_ref = spec.get("claimRef", {}) or {}
        volume_owner = claim_ref.get("namespace", "")

        entry = {
            "pvName": pv_name,
            "imageName": image_name,
            "volumeOwner": volume_owner,
            "pool": vol_attrs.get("pool", ""),
        }
        pv_by_image[image_name] = entry
        all_rbd_pvs.append(entry)

    return pv_by_image, all_rbd_pvs


# ---------------------------------------------------------------------------
# VolumeSnapshotContent list
# ---------------------------------------------------------------------------

UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I
)


def _extract_uuid_from_handle(handle):
    if not handle:
        return None
    matches = UUID_RE.findall(handle)
    return matches[-1] if matches else None


def parse_vsc_json(filepath):
    vsc_by_image = {}

    if not os.path.exists(filepath):
        return vsc_by_image

    with open(filepath, "r", errors="replace") as fh:
        try:
            data = json.load(fh)
        except json.JSONDecodeError:
            return vsc_by_image

    for vsc in data.get("items", []):
        if not isinstance(vsc, dict):
            continue

        spec = vsc.get("spec", {}) or {}
        status = vsc.get("status", {}) or {}
        driver = spec.get("driver", "") or ""

        if "rbd" not in driver.lower():
            continue

        vsc_name = (vsc.get("metadata", {}) or {}).get("name", "")

        snap_handle = status.get("snapshotHandle", "") or ""
        snap_uuid = _extract_uuid_from_handle(snap_handle)
        if not snap_uuid:
            continue

        snap_image_name = f"csi-snap-{snap_uuid}"

        source_handle = (spec.get("source", {}) or {}).get("volumeHandle", "") or ""
        source_uuid = _extract_uuid_from_handle(source_handle)
        source_image_name = f"csi-vol-{source_uuid}" if source_uuid else ""

        vs_ref = spec.get("volumeSnapshotRef", {}) or {}
        volume_owner = vs_ref.get("namespace", "")

        vsc_by_image[snap_image_name] = {
            "snapContentName": vsc_name,
            "imageName": snap_image_name,
            "source": source_image_name,
            "volumeOwner": volume_owner,
        }

    return vsc_by_image


# ---------------------------------------------------------------------------
# Pool backfill
# ---------------------------------------------------------------------------

def backfill_pools(images, default_pool):
    for info in images.values():
        if not info.get("pool"):
            info["pool"] = info.get("parent_pool", "") or default_pool


# ---------------------------------------------------------------------------
# Snapshot reconciliation  (THE CRITICAL FIX)
#
# Problem: rbd snap ls may return an incomplete list (e.g. without --all,
# or if the parent is in trash, or due to Ceph version differences).
# But every CHILD image knows its parent via rbd info "parent" field:
#   parent: {image: "parent-img", snapshot: "snap-name"}
#
# If a child references (parent_image=X, parent_snap=Y) but image X's
# snapshot list doesn't contain snap Y, the tree builder would fail to
# attach the child and it becomes an orphaned root.
#
# Fix: Before building the tree, scan all parent references from children.
# If the referenced snapshot is missing from the parent's snap list,
# synthesize it. This guarantees every parent-child link can be resolved.
# ---------------------------------------------------------------------------

def reconcile_snapshots(images, snapshots):
    """
    Ensure every snapshot referenced by a child's parent field exists in
    the parent's snapshot list. If missing, create a synthetic entry.
    """
    synthesized = 0

    for img_name, info in images.items():
        parent_image = info.get("parent_image")
        parent_snap = info.get("parent_snap")

        if not parent_image or not parent_snap:
            continue

        # Check if the parent image even exists in our data
        if parent_image not in images:
            continue

        # Get parent's current snapshot list
        parent_snaps = snapshots.get(parent_image, [])
        snap_names = {s.get("name", "") for s in parent_snaps}

        # If the referenced snapshot is missing, synthesize it
        if parent_snap not in snap_names:
            if parent_image not in snapshots:
                snapshots[parent_image] = []

            snapshots[parent_image].append({
                "id": "?",
                "name": parent_snap,
                "size": 0,
                "protected": "true",
                "timestamp": "(synthesized from child parent reference)",
            })
            synthesized += 1

    return synthesized


# ---------------------------------------------------------------------------
# Tree builder
# ---------------------------------------------------------------------------

def build_tree(images, snapshots, trash_by_name, pv_by_image, all_rbd_pvs,
               vsc_by_image):
    """
    Build nested parent → snapshot → child tree.

    1. Reconcile snapshots: ensure all parent-referenced snaps exist.
    2. Index children by (parent_image, parent_snap).
    3. Roots = images with no parent.
    4. Recurse: attach children under each snapshot node.
    5. Unvisited images → extra roots (broken chains).
    6. PVs with no image → orphaned.
    """

    # --- CRITICAL: reconcile before building ---
    synth_count = reconcile_snapshots(images, snapshots)
    if synth_count > 0:
        print(
            f"[info] Reconciled snaps: {synth_count} missing snapshot(s) "
            f"synthesized from child parent references",
            file=sys.stderr,
        )

    # --- Index children by parent ---
    children_of_snap = defaultdict(list)

    for img_name, info in images.items():
        parent_image = info.get("parent_image")
        parent_snap = info.get("parent_snap")
        if parent_image:
            children_of_snap[(parent_image, parent_snap)].append(img_name)

    visited = set()

    def _build_node(img_name):
        if img_name in visited:
            return None
        visited.add(img_name)

        info = images.get(img_name, {})

        node = {
            "imageId": info.get("imageId", trash_by_name.get(img_name, "")),
            "imageName": img_name,
            "trash": img_name in trash_by_name,
            "namespace": info.get("namespace", ""),
            "pool": info.get("pool", ""),
            "pv": None,
            "snapshotContent": None,
            "snapshots": [],
        }

        if img_name in pv_by_image:
            pv = pv_by_image[img_name]
            node["pv"] = {
                "pvName": pv["pvName"],
                "imageName": pv["imageName"],
                "volumeOwner": pv["volumeOwner"],
            }

        if img_name in vsc_by_image:
            node["snapshotContent"] = vsc_by_image[img_name]

        # Attach snapshots and recurse into children
        img_snaps = snapshots.get(img_name, [])
        for snap in img_snaps:
            snap_name = snap.get("name", "")
            snap_id = snap.get("id", "")

            snap_node = {
                "snapId": str(snap_id),
                "snapName": snap_name,
                "children": [],
            }

            for child_name in children_of_snap.get((img_name, snap_name), []):
                child = _build_node(child_name)
                if child:
                    snap_node["children"].append(child)

            node["snapshots"].append(snap_node)

        # Children with unknown parent snapshot (parent_snap is None)
        for child_name in children_of_snap.get((img_name, None), []):
            if len(img_snaps) == 1 and node["snapshots"]:
                child = _build_node(child_name)
                if child:
                    node["snapshots"][0]["children"].append(child)
                continue

            child = _build_node(child_name)
            if child:
                node["snapshots"].append({
                    "snapId": "unknown",
                    "snapName": "[parent-snap-unknown]",
                    "children": [child],
                })

        return node

    # --- Roots: images with no parent ---
    root_names = [
        n for n, info in images.items() if "parent_image" not in info
    ]

    volumes = []
    for name in sorted(root_names):
        node = _build_node(name)
        if node:
            volumes.append(node)

    # --- Unvisited (broken parent chains) become extra roots ---
    for name in sorted(images.keys()):
        if name not in visited:
            node = _build_node(name)
            if node:
                volumes.append(node)

    # --- Orphaned PVs ---
    orphaned_pvs = []
    for pv in all_rbd_pvs:
        if pv["imageName"] not in images:
            orphaned_pvs.append({
                "pv_name": pv["pvName"],
                "imageName": pv["imageName"],
                "pool": pv["pool"],
                "namespace": pv["volumeOwner"],
            })

    return {
        "orphaned_pv": orphaned_pvs,
        "volumes": volumes,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Build Ceph RBD parent-child-snapshot tree from live-capture data.",
    )
    parser.add_argument(
        "capture_dir",
        help="Path to capture directory produced by capture_rbd_data.sh.",
    )
    parser.add_argument(
        "--pool", "-p",
        default="ocs-storagecluster-cephblockpool",
        help="Default pool name (fallback when rbd info lacks pool field).",
    )
    parser.add_argument(
        "--output", "-o",
        default=None,
        help="Write JSON output to this file (default: stdout).",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        default=True,
        help="Pretty-print JSON output (default: true).",
    )
    args = parser.parse_args()

    capture_dir = args.capture_dir

    trash_file = os.path.join(capture_dir, "trash_list.json")
    images_file = os.path.join(capture_dir, "all_images.txt")
    pv_file = os.path.join(capture_dir, "pv_list.json")
    vsc_file = os.path.join(capture_dir, "vsc_list.json")

    if not os.path.exists(images_file):
        print(
            f"ERROR: '{images_file}' not found.\n"
            f"       Run capture_rbd_data.sh first.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"[info] Capture dir     : {capture_dir}", file=sys.stderr)

    trash_by_name = parse_trash_list(trash_file)
    print(f"[info] Trashed images  : {len(trash_by_name)}", file=sys.stderr)

    images, snaps = parse_all_images(images_file)
    print(
        f"[info] Images parsed   : {len(images)} total, "
        f"{len(snaps)} with snapshots",
        file=sys.stderr,
    )

    backfill_pools(images, args.pool)

    pv_by_image, all_rbd_pvs = parse_pv_json(pv_file)
    print(f"[info] RBD-backed PVs  : {len(all_rbd_pvs)}", file=sys.stderr)

    vsc_by_image = parse_vsc_json(vsc_file)
    if os.path.exists(vsc_file):
        print(f"[info] SnapshotContents: {len(vsc_by_image)}", file=sys.stderr)
    else:
        print("[info] SnapshotContents: (vsc_list.json not found, skipping)",
              file=sys.stderr)

    result = build_tree(
        images, snaps, trash_by_name, pv_by_image, all_rbd_pvs, vsc_by_image
    )

    total_orphaned = len(result["orphaned_pv"])
    total_roots = len(result["volumes"])
    print(
        f"[info] Result: {total_roots} root volume(s), "
        f"{total_orphaned} orphaned PV(s)",
        file=sys.stderr,
    )

    indent = 2 if args.pretty else None
    output_json = json.dumps(result, indent=indent)

    if args.output:
        with open(args.output, "w") as fh:
            fh.write(output_json + "\n")
        print(f"[info] Written to {args.output}", file=sys.stderr)
    else:
        print(output_json)


if __name__ == "__main__":
    main()