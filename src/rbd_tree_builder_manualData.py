#!/usr/bin/env python3
"""
rbd_tree_builder_live.py
========================
Build Ceph RBD parent-child-snapshot relationship tree from data captured by
capture_rbd_data.sh on a live cluster.

Reads:
  <capture_dir>/trash_list.json        — rbd trash ls output (JSON array)
  <capture_dir>/images_and_snaps.txt   — delimited image info + snap blocks
  <capture_dir>/pv_list.json           — kubectl get pv -o json output

Produces the same nested JSON tree as rbd_tree_builder.py (the must-gather version).

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
# Trash list parser  (same JSON format as must-gather)
# ---------------------------------------------------------------------------

def parse_trash_list(filepath):
    """Parse trash_list.json → {image_name: image_id}"""
    trash_by_name = {}
    if not os.path.exists(filepath):
        return trash_by_name

    with open(filepath, "r", errors="replace") as fh:
        try:
            data = json.load(fh)
        except json.JSONDecodeError:
            return trash_by_name

    for item in data:
        name = item.get("name", "")
        img_id = item.get("id", "")
        if name:
            trash_by_name[name] = img_id

    return trash_by_name


# ---------------------------------------------------------------------------
# images_and_snaps.txt parser  (delimited block format)
# ---------------------------------------------------------------------------

def parse_images_and_snaps(filepath):
    """
    Parse the delimited capture file produced by capture_rbd_data.sh.

    Block format:
        ---IMAGE_START---
        name=<imageName>
        source=pool|trash
        [trash_id=<id>]
        pool=<pool>
        ---INFO---
        <rbd info text output>
        ---SNAPS---
        <json array of snapshots>
        ---IMAGE_END---

    Returns:
        images    : dict  {image_name: {imageId, imageName, pool, namespace,
                           parent_pool, parent_image, parent_snap, ...}}
        snapshots : dict  {image_name: [{id, name, size, protected, ...}]}
    """
    images = {}
    snapshots = {}

    if not os.path.exists(filepath):
        return images, snapshots

    with open(filepath, "r", errors="replace") as fh:
        content = fh.read()

    # Split into blocks between IMAGE_START and IMAGE_END
    blocks = re.split(r"---IMAGE_START---", content)

    for block in blocks:
        block = block.strip()
        if not block:
            continue

        # Strip trailing IMAGE_END
        end_idx = block.find("---IMAGE_END---")
        if end_idx != -1:
            block = block[:end_idx]

        # -- Parse header fields (before ---INFO---) --
        info_split = block.split("---INFO---", 1)
        if len(info_split) < 2:
            continue

        header_text = info_split[0].strip()
        rest = info_split[1]

        headers = {}
        for line in header_text.splitlines():
            line = line.strip()
            if "=" in line:
                key, val = line.split("=", 1)
                headers[key.strip()] = val.strip()

        image_name = headers.get("name", "")
        if not image_name:
            continue

        pool = headers.get("pool", "")
        source = headers.get("source", "pool")
        trash_id = headers.get("trash_id", "")

        # -- Split INFO and SNAPS sections --
        snaps_split = rest.split("---SNAPS---", 1)
        info_text = snaps_split[0].strip() if len(snaps_split) >= 1 else ""
        snaps_text = snaps_split[1].strip() if len(snaps_split) >= 2 else "[]"

        # -- Parse image info (same text format as rbd info output) --
        img_info = _parse_rbd_info_text(info_text, pool, image_name)

        # If image came from trash, and we have the trash_id, use it if
        # rbd info didn't return an id (e.g. info unavailable)
        if trash_id and not img_info.get("imageId"):
            img_info["imageId"] = trash_id

        images[image_name] = img_info

        # -- Parse snapshots JSON --
        snap_list = _parse_snap_json(snaps_text)
        if snap_list:
            snapshots[image_name] = snap_list

    return images, snapshots


def _parse_rbd_info_text(text, pool, image_name):
    """Extract fields from rbd info text output."""
    info = {
        "imageName": image_name,
        "pool": pool,
        "namespace": "",
    }

    if "(info unavailable)" in text:
        info["imageId"] = ""
        return info

    # imageId
    m = re.search(r"^\s+id:\s*(\S+)", text, re.MULTILINE)
    if m:
        info["imageId"] = m.group(1)
    else:
        info["imageId"] = ""

    # snapshot_count
    m = re.search(r"^\s+snapshot_count:\s*(\d+)", text, re.MULTILINE)
    if m:
        info["snapshot_count"] = int(m.group(1))
    else:
        info["snapshot_count"] = 0

    # parent  (format: pool/image@snap  or  pool/image)
    m = re.search(r"^\s+parent:\s*(.+)", text, re.MULTILINE)
    if m:
        parent_str = m.group(1).strip()
        if "@" in parent_str:
            pool_image_part, snap_part = parent_str.rsplit("@", 1)
            info["parent_snap"] = snap_part
        else:
            pool_image_part = parent_str
            info["parent_snap"] = None

        if "/" in pool_image_part:
            info["parent_pool"], info["parent_image"] = pool_image_part.split("/", 1)
        else:
            info["parent_pool"] = pool
            info["parent_image"] = pool_image_part

    return info


def _parse_snap_json(text):
    """Parse JSON snapshot array, tolerating trailing text."""
    text = text.strip()
    if not text or text == "[]":
        return []

    idx = text.find("[")
    if idx == -1:
        return []

    # Find matching closing bracket
    depth = 0
    end = idx
    for i in range(idx, len(text)):
        if text[i] == "[":
            depth += 1
        elif text[i] == "]":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    try:
        return json.loads(text[idx:end])
    except json.JSONDecodeError:
        return []


# ---------------------------------------------------------------------------
# PV JSON parser  (kubectl get pv -o json)
# ---------------------------------------------------------------------------

def parse_pv_json(filepath):
    """
    Parse 'kubectl get pv -o json' output.

    Returns:
        pv_by_image : dict  {imageName: {pvName, imageName, volumeOwner, pool}}
        all_rbd_pvs : list  [same dicts]
    """
    pv_by_image = {}
    all_rbd_pvs = []

    if not os.path.exists(filepath):
        return pv_by_image, all_rbd_pvs

    with open(filepath, "r", errors="replace") as fh:
        try:
            data = json.load(fh)
        except json.JSONDecodeError:
            return pv_by_image, all_rbd_pvs

    items = data.get("items", [])

    for pv in items:
        if not isinstance(pv, dict):
            continue

        spec = pv.get("spec", {}) or {}
        csi = spec.get("csi", {}) or {}
        driver = csi.get("driver", "") or ""

        # Only RBD PVs
        if "rbd" not in driver.lower():
            continue

        vol_attrs = csi.get("volumeAttributes", {}) or {}
        image_name = vol_attrs.get("imageName", "")
        pool = vol_attrs.get("pool", "")

        if not image_name:
            continue

        pv_name = (pv.get("metadata", {}) or {}).get("name", "")
        claim_ref = spec.get("claimRef", {}) or {}
        volume_owner = claim_ref.get("namespace", "")

        entry = {
            "pvName": pv_name,
            "imageName": image_name,
            "volumeOwner": volume_owner,
            "pool": pool,
        }
        pv_by_image[image_name] = entry
        all_rbd_pvs.append(entry)

    return pv_by_image, all_rbd_pvs


# ---------------------------------------------------------------------------
# Tree builder  (identical logic to rbd_tree_builder.py)
# ---------------------------------------------------------------------------

def build_tree(images, snapshots, trash_by_name, pv_by_image, all_rbd_pvs):
    """
    Construct the nested JSON tree.

    1. Index every child image by its (parent_image, parent_snap) pair.
    2. Root images = those with no parent field.
    3. Recursively attach children under each snapshot of each image.
    4. Unvisited images (broken parent chains) become additional roots.
    5. PVs whose imageName has no entry in images → orphaned.
    """
    # (parent_image_name, snap_name) -> [child_image_name, ...]
    children_of_snap = defaultdict(list)

    for img_name, info in images.items():
        parent_image = info.get("parent_image")
        parent_snap = info.get("parent_snap")
        if parent_image:
            children_of_snap[(parent_image, parent_snap)].append(img_name)

    visited = set()

    def _build_node(img_name):
        if img_name in visited:
            return None  # prevent cycles
        visited.add(img_name)

        info = images.get(img_name, {})

        node = {
            "imageId": info.get("imageId", trash_by_name.get(img_name, "")),
            "imageName": img_name,
            "trash": img_name in trash_by_name,
            "namespace": info.get("namespace", ""),
            "pool": info.get("pool", ""),
            "pv": None,
            "snapshots": [],
        }

        # Attach PV reference
        if img_name in pv_by_image:
            pv = pv_by_image[img_name]
            node["pv"] = {
                "pvName": pv["pvName"],
                "imageName": pv["imageName"],
                "volumeOwner": pv["volumeOwner"],
            }

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

            # Children cloned from this exact snapshot
            for child_name in children_of_snap.get((img_name, snap_name), []):
                child = _build_node(child_name)
                if child:
                    snap_node["children"].append(child)

            node["snapshots"].append(snap_node)

        # Children whose parent_snap is None (parent line without @snap)
        for child_name in children_of_snap.get((img_name, None), []):
            if len(img_snaps) == 1:
                existing = node["snapshots"][0] if node["snapshots"] else None
                if existing:
                    child = _build_node(child_name)
                    if child:
                        existing["children"].append(child)
                    continue

            child = _build_node(child_name)
            if child:
                synthetic_snap = {
                    "snapId": "unknown",
                    "snapName": "[parent-snap-unknown]",
                    "children": [child],
                }
                node["snapshots"].append(synthetic_snap)

        return node

    # --- Identify roots (images with no parent) ---
    root_names = [
        name for name, info in images.items() if "parent_image" not in info
    ]

    volumes = []
    for name in sorted(root_names):
        node = _build_node(name)
        if node:
            volumes.append(node)

    # --- Catch any unvisited images (broken parent chains) ---
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
        description=(
            "Build Ceph RBD parent-child-snapshot tree from live-capture data "
            "(produced by capture_rbd_data.sh)."
        ),
    )
    parser.add_argument(
        "capture_dir",
        help="Path to capture directory containing trash_list.json, "
             "images_and_snaps.txt, and pv_list.json.",
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

    # --- Validate input files ---
    trash_file = os.path.join(capture_dir, "trash_list.json")
    info_file = os.path.join(capture_dir, "images_and_snaps.txt")
    pv_file = os.path.join(capture_dir, "pv_list.json")

    missing = []
    for f in [trash_file, info_file, pv_file]:
        if not os.path.exists(f):
            missing.append(os.path.basename(f))

    if missing:
        print(
            f"ERROR: Missing file(s) in '{capture_dir}': {', '.join(missing)}\n"
            f"       Run capture_rbd_data.sh first to generate these files.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"[info] Capture dir     : {capture_dir}", file=sys.stderr)

    # --- Parse trash list ---
    trash_by_name = parse_trash_list(trash_file)
    print(f"[info] Trashed images  : {len(trash_by_name)}", file=sys.stderr)

    # --- Parse image + snap info ---
    images, snaps = parse_images_and_snaps(info_file)
    print(
        f"[info] Images parsed   : {len(images)} total, "
        f"{len(snaps)} with snapshots",
        file=sys.stderr,
    )

    # --- Parse PV list ---
    pv_by_image, all_rbd_pvs = parse_pv_json(pv_file)
    print(f"[info] RBD-backed PVs  : {len(all_rbd_pvs)}", file=sys.stderr)

    # --- Build the tree ---
    result = build_tree(images, snaps, trash_by_name, pv_by_image, all_rbd_pvs)

    total_orphaned = len(result["orphaned_pv"])
    total_roots = len(result["volumes"])
    print(
        f"[info] Result: {total_roots} root volume(s), {total_orphaned} orphaned PV(s)",
        file=sys.stderr,
    )

    # --- Output ---
    indent = 2 if args.pretty else None
    output_json = json.dumps(result, indent=indent)

    if args.output:
        with open(args.output, "w") as fh:
            fh.write(output_json)
            fh.write("\n")
        print(f"[info] Written to {args.output}", file=sys.stderr)
    else:
        print(output_json)


if __name__ == "__main__":
    main()