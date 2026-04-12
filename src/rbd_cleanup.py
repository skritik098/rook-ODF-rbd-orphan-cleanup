#!/usr/bin/env python3
"""
RBD Orphan Cleanup Tool

Reads the JSON tree produced by rbd_tree_builder.py, identifies orphan images
(those with both pv=null and snapshotContent=null), and deletes them bottom-up.

When an orphan parent has a non-orphan child (has PV/snapshotContent), the tool
offers to flatten the child first to break the dependency. If the parent is in
trash, it is restored before flattening and deleted after cleanup.

Usage:
    python3 rbd_cleanup.py <input.json>
    python3 rbd_cleanup.py --dry-run <input.json>
"""

import json
import sys
import subprocess
import argparse


# ─── Helpers ────────────────────────────────────────────────────────────────────

def log(msg):
    print(f"[INFO] {msg}", file=sys.stderr)


def warn(msg):
    print(f"[WARN] {msg}", file=sys.stderr)


def error(msg):
    print(f"[ERROR] {msg}", file=sys.stderr)


def ns_args(namespace):
    if namespace:
        return ["--namespace", namespace]
    return []


def run_cmd(cmd, dry_run=False):
    """Execute a command. Returns (success, stderr_text)."""
    cmd_str = " ".join(cmd)
    if dry_run:
        log(f"  [DRY-RUN] {cmd_str}")
        return True, ""

    log(f"  Executing: {cmd_str}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    except subprocess.TimeoutExpired:
        return False, "command timed out"
    if result.returncode != 0:
        return False, result.stderr.strip()
    return True, ""


def is_orphan(node):
    """Image has no PV and no snapshotContent reference."""
    return node.get("pv") is None and node.get("snapshotContent") is None


# ─── Display ────────────────────────────────────────────────────────────────────

def show_image_banner(node, pool, action_label):
    ns = node.get("namespace", "")
    ns_display = ns if ns else "(default)"
    trash_label = " [TRASH]" if node.get("trash") else ""
    snaps = node.get("snapshots", [])

    print(f"\n{'='*70}")
    print(f"  {action_label}: {node['imageName']}{trash_label}")
    print(f"  Image ID  : {node['imageId']}")
    print(f"  Pool      : {pool}")
    print(f"  Namespace : {ns_display}")
    print(f"  PV        : {node.get('pv')}")
    print(f"  SnapContent: {node.get('snapshotContent')}")

    if snaps:
        print(f"  Snapshots ({len(snaps)}):")
        for s in snaps:
            children = s.get("children", [])
            child_names = ", ".join(c["imageName"] for c in children) if children else "none"
            print(f"    • {s['snapName']}  (snap-id: {s['snapId']})  children: [{child_names}]")
    else:
        print("  Snapshots : (none)")

    print(f"{'='*70}")


# ─── Trash Restore ─────────────────────────────────────────────────────────────

def restore_parent_from_trash(parent, pool, dry_run):
    """Restore a trashed parent so flatten can read from it."""
    ns = parent.get("namespace", "")
    log(f"  Restoring parent '{parent['imageName']}' from trash (required for flatten)...")
    ok, err = run_cmd(
        ["rbd", "trash", "restore", "-p", pool]
        + ns_args(ns)
        + [parent["imageId"]],
        dry_run=dry_run,
    )
    if not ok:
        error(f"  Failed to restore parent from trash: {err}")
        return False
    return True


# ─── Flatten Logic ─────────────────────────────────────────────────────────────

def flatten_child(child, pool, dry_run):
    """Flatten a non-orphan child to detach it from its parent."""
    ns = child.get("namespace", "")
    child_name = child["imageName"]

    log(f"  Flattening '{child_name}'...")
    ok, err = run_cmd(
        ["rbd", "flatten", "-p", pool]
        + ns_args(ns)
        + [child_name],
        dry_run=dry_run,
    )
    if not ok:
        error(f"  Flatten failed for '{child_name}': {err}")
        return False

    log(f"  ✓ Flattened '{child_name}'")
    return True


# ─── Deletion Logic ────────────────────────────────────────────────────────────

def get_live_snap_ids(pool, namespace, image_id, dry_run):
    """Query the cluster for snapshots that currently exist on this image."""
    if dry_run:
        return None  # can't query in dry-run, assume all exist
    cmd = (
        ["rbd", "snap", "ls", "-p", pool]
        + ns_args(namespace)
        + ["--image-id", image_id, "--all", "--format", "json"]
    )
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except subprocess.TimeoutExpired:
        return None
    if result.returncode != 0:
        return None
    try:
        data = json.loads(result.stdout.strip())
        return {str(s.get("id", "")) for s in data}
    except (json.JSONDecodeError, TypeError):
        return None


def image_exists(pool, namespace, image_id):
    """Check if an image still exists on the cluster (regular or trash)."""
    cmd = (
        ["rbd", "info", "-p", pool]
        + ns_args(namespace)
        + ["--image-id", image_id, "--format", "json"]
    )
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except subprocess.TimeoutExpired:
        return True  # assume exists on timeout
    return result.returncode == 0


def delete_image_snapshots_and_self(node, pool, dry_run):
    """
    Remove all snapshots (unprotect → rm each), then remove the image.
    Assumes every child clone has already been removed or flattened.
    If the image was auto-purged by Ceph (e.g. trash image whose last
    snapshot child was removed), skip everything and treat as success.
    """
    ns = node.get("namespace", "")
    image_id = node["imageId"]
    image_name = node["imageName"]
    is_trash = node.get("trash", False)

    # ── Check if the image itself still exists ──
    if not dry_run and not image_exists(pool, ns, image_id):
        log(f"  Image '{image_name}' (id={image_id}) already auto-purged, nothing to do.")
        return True

    # ── Check which snapshots still exist on the cluster ──
    live_snaps = get_live_snap_ids(pool, ns, image_id, dry_run)

    # ── Snapshots ──
    for snap in node.get("snapshots", []):
        snap_id = snap["snapId"]
        snap_name = snap["snapName"]

        # Skip if snapshot no longer exists (auto-purged after flatten)
        if live_snaps is not None and snap_id not in live_snaps:
            log(f"  Snapshot '{snap_name}' (snap-id: {snap_id}) already removed, skipping.")
            continue

        # Unprotect (best-effort, uses --snap <name> not --snap-id)
        ok, err = run_cmd(
            ["rbd", "snap", "unprotect", "-p", pool]
            + ns_args(ns)
            + ["--image-id", image_id, "--snap", snap_name],
            dry_run=dry_run,
        )
        if not ok:
            lower = err.lower()
            if "not protected" not in lower and "no such" not in lower:
                warn(f"  Unprotect snap '{snap_name}' failed: {err}")

        # Remove snapshot
        ok, err = run_cmd(
            ["rbd", "snap", "rm", "-p", pool]
            + ns_args(ns)
            + ["--image-id", image_id, "--snap", snap_name],
            dry_run=dry_run,
        )
        if not ok:
            error(f"  Failed to remove snapshot '{snap_name}': {err}")
            return False

    # ── Image ──
    if is_trash:
        ok, err = run_cmd(
            ["rbd", "trash", "remove", "-p", pool]
            + ns_args(ns)
            + [image_id],
            dry_run=dry_run,
        )
    else:
        ok, err = run_cmd(
            ["rbd", "rm", "-p", pool]
            + ns_args(ns)
            + [image_name],
            dry_run=dry_run,
        )

    if not ok:
        error(f"  Failed to remove image '{image_name}': {err}")
        return False

    log(f"  ✓ Removed image '{image_name}' (id={image_id})")
    return True


# ─── Recursive Bottom-Up Processor ─────────────────────────────────────────────

def process_node(node, pool, dry_run, counters):
    """
    Recursively process a node bottom-up through the ENTIRE depth of the tree.

    For orphan images:
      1. Process each snapshot's children first (bottom-up).
         - Orphan children    → recurse (delete them).
         - Non-orphan children → recurse into THEIR subtree first (to handle
           deeper orphans), then offer to flatten.
      2. If all children are cleared (deleted or flattened), delete this image.

    For non-orphan images:
      - Skip this image, but recurse into ALL descendants to find orphans at
        any depth.

    Returns True if the node was fully cleaned up.
    """
    p = node.get("pool", pool)

    if not is_orphan(node):
        # Not orphan — recurse through ALL snapshots and children at every level
        for snap in node.get("snapshots", []):
            for child in snap.get("children", []):
                process_node(child, p, dry_run, counters)
        return False

    # ── This node IS orphan — process children first (bottom-up) ──
    parent_restored = False
    all_children_cleared = True

    for snap in node.get("snapshots", []):
        for child in snap.get("children", []):
            if is_orphan(child):
                # Orphan child → recursively clean it (goes all the way down)
                if not process_node(child, p, dry_run, counters):
                    all_children_cleared = False
            else:
                # ── Non-orphan child ──
                # Step A: recurse into child's ENTIRE subtree first to handle
                #         any deeper orphans before we flatten this child
                process_node(child, p, dry_run, counters)

                # Step B: now offer to flatten this non-orphan child
                show_image_banner(child, p, "NON-ORPHAN CHILD (flatten candidate)")
                print(f"  Parent (orphan): {node['imageName']} (id={node['imageId']})")

                if dry_run:
                    log(f"  [DRY-RUN] Would offer to flatten '{child['imageName']}'")
                    # Restore parent from trash if needed (once per parent)
                    if node.get("trash") and not parent_restored:
                        restore_parent_from_trash(node, p, dry_run=True)
                        parent_restored = True
                        node["trash"] = False
                    flatten_child(child, p, dry_run=True)
                    counters["flattened"] += 1
                    continue

                answer = input(
                    f"\n  Flatten '{child['imageName']}' to detach from orphan parent? (y/n): "
                ).strip().lower()

                if answer != "y":
                    log(f"  Skipped flatten for '{child['imageName']}'.")
                    all_children_cleared = False
                    continue

                # Restore parent from trash if needed (once per parent)
                if node.get("trash") and not parent_restored:
                    if not restore_parent_from_trash(node, p, dry_run=False):
                        all_children_cleared = False
                        continue
                    parent_restored = True
                    node["trash"] = False

                if flatten_child(child, p, dry_run=False):
                    counters["flattened"] += 1
                else:
                    all_children_cleared = False

    # ── All children processed — can we delete this image? ──
    if not all_children_cleared:
        log(
            f"  Cannot delete '{node['imageName']}' — "
            f"some children were not removed/flattened. Skipping."
        )
        counters["skipped"] += 1
        return False

    # ── Delete this orphan image ──
    show_image_banner(node, p, "DELETE ORPHAN IMAGE")

    if dry_run:
        log("[DRY-RUN] Simulating deletion...")
        delete_image_snapshots_and_self(node, p, dry_run=True)
        counters["deleted"] += 1
        return True

    answer = input("\n  Proceed with deletion? (y/n): ").strip().lower()
    if answer != "y":
        log("  Skipped.")
        counters["skipped"] += 1
        return False

    if delete_image_snapshots_and_self(node, p, dry_run=False):
        counters["deleted"] += 1
        return True
    else:
        counters["failed"] += 1
        cont = input("  Deletion failed. Continue with next? (y/n): ").strip().lower()
        if cont != "y":
            log("  Aborting.")
            sys.exit(1)
        return False


# ─── Pre-scan Summary ──────────────────────────────────────────────────────────

def count_orphans(node):
    """Count orphan images and flatten candidates across the ENTIRE tree depth."""
    orphans = 0
    flatten_candidates = 0

    if is_orphan(node):
        orphans += 1
        for snap in node.get("snapshots", []):
            for child in snap.get("children", []):
                if is_orphan(child):
                    o, f = count_orphans(child)
                    orphans += o
                    flatten_candidates += f
                else:
                    flatten_candidates += 1
                    # Recurse into non-orphan child to find deeper orphans
                    o, f = count_orphans(child)
                    orphans += o
                    flatten_candidates += f
    else:
        for snap in node.get("snapshots", []):
            for child in snap.get("children", []):
                o, f = count_orphans(child)
                orphans += o
                flatten_candidates += f

    return orphans, flatten_candidates


def print_orphan_tree(node, pool, indent=0):
    """Print a compact tree view across the ENTIRE depth of the tree."""
    p = node.get("pool", pool)
    prefix = "  " * indent
    trash_tag = " [TRASH]" if node.get("trash") else ""

    if is_orphan(node):
        print(f"{prefix}✗ {node['imageName']}{trash_tag}  (id={node['imageId']})  ← ORPHAN (delete)")
    else:
        pv = node.get("pv", {})
        sc = node.get("snapshotContent", {})
        ref = pv.get("pvName") if pv else (sc.get("snapContentName") if sc else None)
        ref_str = f" ref={ref}" if ref else ""
        # Determine label based on context (indent > 0 means it's a child)
        if indent > 0:
            print(
                f"{prefix}✓ {node['imageName']}{trash_tag}  "
                f"(id={node['imageId']})  ← NON-ORPHAN (flatten){ref_str}"
            )
        else:
            # Top-level non-orphan volume — skip display, just recurse
            pass

    for snap in node.get("snapshots", []):
        if snap.get("children"):
            print(f"{prefix}  └─ snap: {snap['snapName']}  (snap-id: {snap['snapId']})")
            for child in snap["children"]:
                print_orphan_tree(child, p, indent + 3)


# ─── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Clean up orphan RBD images using the JSON tree from rbd_tree_builder.py",
    )
    parser.add_argument("input", help="Path to the JSON tree file")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print commands without executing them",
    )
    args = parser.parse_args()

    with open(args.input, "r") as f:
        data = json.load(f)

    volumes = data.get("volumes", [])
    if not volumes:
        log("No volumes found in input. Nothing to clean.")
        sys.exit(0)

    # ── Pre-scan summary ──
    total_orphans = 0
    total_flatten = 0
    for vol in volumes:
        o, f = count_orphans(vol)
        total_orphans += o
        total_flatten += f

    if total_orphans == 0:
        log("No orphan images found. Nothing to clean.")
        sys.exit(0)

    print(f"\n{'='*70}")
    print(f"  Cleanup Plan")
    print(f"  Orphan images to delete        : {total_orphans}")
    print(f"  Non-orphan children to flatten  : {total_flatten}")
    print(f"{'='*70}\n")

    for vol in volumes:
        print_orphan_tree(vol, vol.get("pool", ""))

    print()

    # ── Process each volume tree recursively (bottom-up) ──
    counters = {"deleted": 0, "flattened": 0, "skipped": 0, "failed": 0}

    for vol in volumes:
        process_node(vol, vol.get("pool", ""), args.dry_run, counters)

    # ── Final summary ──
    mode = " (DRY-RUN)" if args.dry_run else ""
    print(f"\n{'='*70}")
    print(f"  Cleanup summary{mode}:")
    print(f"    Deleted   : {counters['deleted']}")
    print(f"    Flattened : {counters['flattened']}")
    print(f"    Skipped   : {counters['skipped']}")
    print(f"    Failed    : {counters['failed']}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()