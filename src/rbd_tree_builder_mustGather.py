#!/usr/bin/env python3
"""
rbd_tree_builder.py
===================
Build Ceph RBD parent-child-snapshot relationship tree from ODF must-gather data.

Parses:
  - rbd_trash_ls_<pool>                       (JSON: trashed images)
  - rbd_vol_and_snap_info_<pool>              (mixed text+JSON: image info & snapshots)
  - cluster-scoped-resources/core/persistentvolumes/*.yaml  (PV -> imageName mapping)

Produces a JSON tree with:
  - orphaned_pv:  PVs whose imageName has no matching RBD image
  - volumes:      nested parent -> snapshot -> child tree

Usage:
    python3 rbd_tree_builder.py <must-gather-dir> [--output output.json]

The <must-gather-dir> can be:
  - The exact must-gather root   (e.g. registry-odf4-odf-must-gather-rhel9-sha256-xxx/)
  - A parent folder containing it (auto-discovered)
"""

import os
import sys
import re
import json
import glob
import argparse
from collections import defaultdict

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML is required. Install with: pip install pyyaml", file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Directory discovery
# ---------------------------------------------------------------------------

def find_must_gather_root(path):
    """Walk downward to find the must-gather root (contains ceph/must_gather_commands)."""
    # Check the path itself first
    candidate = os.path.join(path, "ceph", "must_gather_commands")
    if os.path.isdir(candidate):
        return path

    # Walk one or two levels deep
    for root, dirs, _files in os.walk(path):
        if "ceph" in dirs:
            ceph_cmds = os.path.join(root, "ceph", "must_gather_commands")
            if os.path.isdir(ceph_cmds):
                return root
        # Don't recurse too deep
        depth = root[len(path):].count(os.sep)
        if depth >= 3:
            dirs.clear()

    return path  # fallback


def discover_pools(mg_commands_dir):
    """Discover pool names from rbd_vol_and_snap_info_* file names."""
    pools = []
    for fname in sorted(os.listdir(mg_commands_dir)):
        m = re.match(r"rbd_vol_and_snap_info_(.+)$", fname)
        if m:
            pools.append(m.group(1))
    return pools


# ---------------------------------------------------------------------------
# Trash list parser
# ---------------------------------------------------------------------------

def parse_trash_list(filepath):
    """
    Parse rbd_trash_ls_<pool> file.

    Returns:
        trash_by_name : dict  {image_name: image_id}
    """
    trash_by_name = {}

    if not os.path.exists(filepath):
        return trash_by_name

    with open(filepath, "r", errors="replace") as fh:
        content = fh.read().strip()

    # Locate the JSON array (may have a shell-command preamble)
    idx = content.find("[")
    if idx == -1:
        return trash_by_name

    try:
        data = json.loads(content[idx:])
    except json.JSONDecodeError:
        # Try to find a balanced bracket
        data = _safe_json_array(content, idx)

    if data is None:
        return trash_by_name

    for item in data:
        name = item.get("name", "")
        img_id = item.get("id", "")
        if name:
            trash_by_name[name] = img_id

    return trash_by_name


# ---------------------------------------------------------------------------
# Vol-and-snap-info parser
# ---------------------------------------------------------------------------

SECTION_RE = re.compile(
    r"^(Collecting\s+(?:image info|image status|snap info)\s+for:\s*(.+))$",
    re.MULTILINE,
)


def parse_vol_and_snap_info(filepath):
    """
    Parse rbd_vol_and_snap_info_<pool>.

    Returns:
        images    : dict  {image_name: {imageId, imageName, pool, namespace,
                           parent_pool, parent_image, parent_snap, snapshot_count}}
        snapshots : dict  {image_name: [{id, name, size, protected, timestamp, ...}]}
    """
    images = {}
    snapshots = {}

    if not os.path.exists(filepath):
        return images, snapshots

    with open(filepath, "r", errors="replace") as fh:
        content = fh.read()

    matches = list(SECTION_RE.finditer(content))

    for i, m in enumerate(matches):
        header = m.group(1).strip()
        pool_image = m.group(2).strip()

        # Split pool / image_name
        if "/" in pool_image:
            pool, image_name = pool_image.split("/", 1)
        else:
            pool = ""
            image_name = pool_image

        # Section body runs until the next header (or EOF)
        body_start = m.end()
        body_end = matches[i + 1].start() if i + 1 < len(matches) else len(content)
        body = content[body_start:body_end]

        if "Collecting image info for:" in header:
            img_info = _parse_image_info_block(body, pool, image_name)
            if img_info:
                images[image_name] = img_info

        elif "Collecting snap info for:" in header:
            snap_list = _parse_snap_json(body)
            if snap_list:
                snapshots[image_name] = snap_list
        # "image status" sections are ignored (not needed for the tree)

    return images, snapshots


def _parse_image_info_block(text, pool, image_name):
    """Extract fields from an 'rbd image ...' text block."""
    info = {
        "imageName": image_name,
        "pool": pool,
        "namespace": "",
    }

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
            info["parent_snap"] = None  # unknown snap

        if "/" in pool_image_part:
            info["parent_pool"], info["parent_image"] = pool_image_part.split("/", 1)
        else:
            info["parent_pool"] = pool
            info["parent_image"] = pool_image_part

    return info


def _parse_snap_json(text):
    """Extract the JSON snapshot array from a snap-info section body."""
    idx = text.find("[")
    if idx == -1:
        return []

    data = _safe_json_array(text, idx)
    return data if data else []


def _safe_json_array(text, start):
    """Parse a JSON array starting at *start*, tolerating trailing garbage."""
    depth = 0
    end = start
    for i in range(start, len(text)):
        if text[i] == "[":
            depth += 1
        elif text[i] == "]":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    try:
        return json.loads(text[start:end])
    except json.JSONDecodeError:
        return None


# ---------------------------------------------------------------------------
# PV YAML parser
# ---------------------------------------------------------------------------

def parse_pv_files(pv_dir):
    """
    Parse all PV YAML files looking for RBD-backed PVs.

    Returns:
        pv_by_image : dict  {imageName: {pvName, imageName, volumeOwner, pool}}
        all_rbd_pvs : list  [same dicts]
    """
    pv_by_image = {}
    all_rbd_pvs = []

    if not os.path.isdir(pv_dir):
        return pv_by_image, all_rbd_pvs

    for yaml_file in sorted(glob.glob(os.path.join(pv_dir, "*.yaml"))):
        try:
            with open(yaml_file, "r") as fh:
                pv = yaml.safe_load(fh)
        except Exception:
            continue

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

        # Fallback: try to extract imageName from nodeStageSecretRef or volumeHandle
        if not image_name:
            vol_handle = csi.get("volumeHandle", "")
            # Some CSI drivers encode imageName in volumeHandle
            # Format varies; try a common pattern: ...-<pool>-<imageName>
            # Not always reliable, so skip if we can't confidently extract it
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
# Tree builder
# ---------------------------------------------------------------------------

def build_tree(images, snapshots, trash_by_name, pv_by_image, all_rbd_pvs):
    """
    Construct the nested JSON tree.

    Strategy
    --------
    1. Index every child image by its (parent_image, parent_snap) pair.
    2. Root images are those with no parent field.
    3. Recursively attach children under each snapshot of each image.
    4. Any image not yet visited (e.g. parent missing from data) becomes an
       additional root so nothing is silently dropped.
    5. PVs whose imageName has no entry in *images* are reported as orphaned.
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

        # Handle children whose parent_snap is None (parent line without @snap)
        # Try to match them to a snapshot by checking if there's only one snap
        for child_name in children_of_snap.get((img_name, None), []):
            # If the parent image has exactly one snapshot, assume it's that one
            if len(img_snaps) == 1:
                existing = node["snapshots"][0] if node["snapshots"] else None
                if existing:
                    child = _build_node(child_name)
                    if child:
                        existing["children"].append(child)
                    continue

            # Otherwise, create a synthetic "[unknown]" snapshot entry
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

    # --- Catch any unvisited images (broken parent chains, etc.) ---
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
        description="Build Ceph RBD parent-child-snapshot tree from ODF must-gather."
    )
    parser.add_argument(
        "must_gather_dir",
        help="Path to must-gather root directory (or parent folder).",
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

    # --- Locate directories ---
    mg_root = find_must_gather_root(args.must_gather_dir)
    ceph_cmds_dir = os.path.join(mg_root, "ceph", "must_gather_commands")
    pv_dir = os.path.join(mg_root, "cluster-scoped-resources", "core", "persistentvolumes")

    if not os.path.isdir(ceph_cmds_dir):
        print(
            f"ERROR: Cannot find ceph/must_gather_commands under '{args.must_gather_dir}'.\n"
            f"       Looked for: {ceph_cmds_dir}",
            file=sys.stderr,
        )
        sys.exit(1)

    # --- Discover pools ---
    pools = discover_pools(ceph_cmds_dir)
    if not pools:
        print("ERROR: No rbd_vol_and_snap_info_* files found.", file=sys.stderr)
        sys.exit(1)

    print(f"[info] Must-gather root : {mg_root}", file=sys.stderr)
    print(f"[info] Pools discovered : {', '.join(pools)}", file=sys.stderr)

    # --- Parse PV files ---
    pv_by_image, all_rbd_pvs = parse_pv_files(pv_dir)
    print(f"[info] RBD-backed PVs  : {len(all_rbd_pvs)}", file=sys.stderr)

    # --- Parse each pool ---
    all_images = {}
    all_snapshots = {}
    all_trash = {}

    for pool in pools:
        trash_file = os.path.join(ceph_cmds_dir, f"rbd_trash_ls_{pool}")
        trash_by_name = parse_trash_list(trash_file)
        all_trash.update(trash_by_name)

        info_file = os.path.join(ceph_cmds_dir, f"rbd_vol_and_snap_info_{pool}")
        images, snaps = parse_vol_and_snap_info(info_file)
        all_images.update(images)
        all_snapshots.update(snaps)

        print(
            f"[info] Pool '{pool}': "
            f"{len(images)} images, "
            f"{len(snaps)} with snapshots, "
            f"{len(trash_by_name)} in trash",
            file=sys.stderr,
        )

    # --- Build the tree ---
    result = build_tree(all_images, all_snapshots, all_trash, pv_by_image, all_rbd_pvs)

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