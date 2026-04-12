#!/usr/bin/env python3
"""
RBD Parent-Child Tree Builder

Discovers all RBD images (including trash) in a pool, resolves snapshot-clone
relationships, and outputs a nested JSON tree rooted at top-level (parentless)
images.

Usage:
    python3 rbd_tree_builder.py
    python3 rbd_tree_builder.py --pool <pool> [--namespace <ns>] [-o output.json]
"""

import subprocess
import json
import sys
import os
import argparse
import tempfile


# ─── Helpers ────────────────────────────────────────────────────────────────────

def log(msg):
    print(f"[INFO] {msg}", file=sys.stderr)


def warn(msg):
    print(f"[WARN] {msg}", file=sys.stderr)


def run_cmd(cmd):
    """Run a shell command, return parsed JSON or None on failure."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    except subprocess.TimeoutExpired:
        warn(f"Timeout: {' '.join(cmd)}")
        return None

    if result.returncode != 0:
        warn(f"Command failed: {' '.join(cmd)}")
        warn(f"  stderr: {result.stderr.strip()}")
        return None

    stdout = result.stdout.strip()
    if not stdout:
        return None

    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        warn(f"Invalid JSON from: {' '.join(cmd)}")
        return None


def ns_args(namespace):
    """Return namespace CLI args; omit entirely for the default namespace."""
    if namespace:
        return ["--namespace", namespace]
    return []


# ─── RBD Command Wrappers ──────────────────────────────────────────────────────

def get_namespaces(pool):
    """Discover all namespaces in a pool (always includes default '')."""
    data = run_cmd(["rbd", "namespace", "ls", "-p", pool, "--format", "json"])
    namespaces = [""]  # default namespace is never listed but always exists
    if data:
        for item in data:
            name = item.get("name", "") if isinstance(item, dict) else str(item)
            if name and name not in namespaces:
                namespaces.append(name)
    return namespaces


def list_images(pool, namespace):
    """List regular (non-trash) image names."""
    cmd = ["rbd", "ls", "-p", pool] + ns_args(namespace) + ["--format", "json"]
    data = run_cmd(cmd)
    if not data:
        return []
    return [
        item.get("image", item.get("name", "")) if isinstance(item, dict) else str(item)
        for item in data
    ]


def list_trash_images(pool, namespace):
    """List trash images as list of dicts with 'id' and 'name'."""
    cmd = ["rbd", "trash", "ls", "-p", pool] + ns_args(namespace) + ["--format", "json"]
    data = run_cmd(cmd)
    return data if data else []


def get_image_info(pool, namespace, image_name=None, image_id=None):
    """Get image info by name or ID."""
    cmd = ["rbd", "info", "-p", pool] + ns_args(namespace) + ["--format", "json"]
    if image_id:
        cmd.extend(["--image-id", image_id])
    elif image_name:
        cmd.append(image_name)
    else:
        return None
    return run_cmd(cmd)


def list_snapshots(pool, namespace, image_id):
    """List all snapshots (including internal) for an image by ID."""
    cmd = (
        ["rbd", "snap", "ls", "-p", pool]
        + ns_args(namespace)
        + ["--image-id", image_id, "--all", "--format", "json"]
    )
    data = run_cmd(cmd)
    return data if data else []


def get_children(pool, namespace, image_id, snap_id):
    """Get children (clones) of a specific snapshot."""
    cmd = (
        ["rbd", "children", "-p", pool]
        + ns_args(namespace)
        + ["--image-id", image_id, "--snap-id", str(snap_id), "--all", "--format", "json"]
    )
    data = run_cmd(cmd)
    return data if data else []


# ─── Core Logic ─────────────────────────────────────────────────────────────────

# ─── RADOS OMAP Helpers ────────────────────────────────────────────────────────

def rados_listomapkeys(pool, obj):
    """List OMAP keys for a RADOS object (default RADOS namespace)."""
    try:
        result = subprocess.run(
            ["rados", "listomapkeys", "-p", pool, obj],
            capture_output=True, text=True, timeout=30,
        )
    except subprocess.TimeoutExpired:
        warn(f"Timeout: rados listomapkeys {obj}")
        return []
    if result.returncode != 0:
        return []
    return [line.strip() for line in result.stdout.strip().split("\n") if line.strip()]


def rados_getomapval(pool, obj, key):
    """
    Get a single OMAP value as a UTF-8 string.
    Uses a temp file because `rados getomapval` writes raw bytes to an output file.
    """
    fd, tmp_path = tempfile.mkstemp(prefix=".omap_")
    os.close(fd)
    try:
        result = subprocess.run(
            ["rados", "getomapval", "-p", pool, obj, key, tmp_path],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            return None
        with open(tmp_path, "rb") as f:
            return f.read().decode("utf-8", errors="replace")
    except (subprocess.TimeoutExpired, OSError):
        return None
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass


def build_csi_lookups(pool):
    """
    Scan RADOS OMAP metadata to build two lookups:
      pv_lookup      : rbd_image_id  →  { pvName, imageName, volumeOwner }
      snap_lookup    : rbd_image_id  →  { snapContentName, imageName, source, volumeOwner }
    """
    pv_lookup = {}
    snap_lookup = {}

    # ── PersistentVolume mappings (csi.volumes.default) ──
    log("Scanning RADOS OMAP: csi.volumes.default")
    vol_keys = rados_listomapkeys(pool, "csi.volumes.default")
    log(f"  Found {len(vol_keys)} PV entries")

    for vk in vol_keys:
        # vk = "csi.volume.pvc-<uuid>"  → value is a volume UUID
        vol_uuid = rados_getomapval(pool, "csi.volumes.default", vk)
        if not vol_uuid:
            warn(f"  Could not read value for key '{vk}', skipping")
            continue

        obj_name = f"csi.volume.{vol_uuid}"
        image_id    = rados_getomapval(pool, obj_name, "csi.imageid")
        image_name  = rados_getomapval(pool, obj_name, "csi.imagename")
        vol_name    = rados_getomapval(pool, obj_name, "csi.volname")
        vol_owner   = rados_getomapval(pool, obj_name, "csi.volume.owner")

        if image_id:
            pv_lookup[image_id] = {
                "pvName": vol_name,
                "imageName": image_name,
                "volumeOwner": vol_owner,
            }

    # ── VolumeSnapshotContent mappings (csi.snaps.default) ──
    log("Scanning RADOS OMAP: csi.snaps.default")
    snap_keys = rados_listomapkeys(pool, "csi.snaps.default")
    log(f"  Found {len(snap_keys)} SnapshotContent entries")

    for sk in snap_keys:
        # sk = "csi.snap.snapshot-<uuid>"  → value is a snap UUID
        snap_uuid = rados_getomapval(pool, "csi.snaps.default", sk)
        if not snap_uuid:
            warn(f"  Could not read value for key '{sk}', skipping")
            continue

        obj_name = f"csi.snap.{snap_uuid}"
        image_id    = rados_getomapval(pool, obj_name, "csi.imageid")
        image_name  = rados_getomapval(pool, obj_name, "csi.imagename")
        snap_name   = rados_getomapval(pool, obj_name, "csi.snapname")
        source      = rados_getomapval(pool, obj_name, "csi.source")
        vol_owner   = rados_getomapval(pool, obj_name, "csi.volume.owner")

        if image_id:
            snap_lookup[image_id] = {
                "snapContentName": snap_name,
                "imageName": image_name,
                "source": source,
                "volumeOwner": vol_owner,
            }

    log(f"  PV lookup entries: {len(pv_lookup)}, SnapshotContent lookup entries: {len(snap_lookup)}")
    return pv_lookup, snap_lookup

def collect_all_images(pool, namespaces):
    """
    Enumerate every image (regular + trash) and store metadata.
    Key: (pool, namespace, image_id)
    """
    all_images = {}

    for ns in namespaces:
        ns_display = ns if ns else "(default)"
        log(f"Scanning namespace: {ns_display}")

        # ── Regular images ──
        for name in list_images(pool, ns):
            info = get_image_info(pool, ns, image_name=name)
            if not info:
                warn(f"Could not get info for image '{name}' in ns '{ns_display}', skipping")
                continue
            img_id = info.get("id", "")
            key = (pool, ns, img_id)
            all_images[key] = {
                "imageId": img_id,
                "imageName": name,
                "trash": False,
                "pool": pool,
                "namespace": ns,
                "parent": info.get("parent"),
                "_snapshots_raw": [],  # filled later
            }

        # ── Trash images ──
        for item in list_trash_images(pool, ns):
            img_id = item.get("id", "")
            img_name = item.get("name", "")
            key = (pool, ns, img_id)
            if key in all_images:
                continue  # already collected (shouldn't happen, but guard)
            info = get_image_info(pool, ns, image_id=img_id)
            if not info:
                warn(f"Could not get info for trash image id '{img_id}' in ns '{ns_display}', skipping")
                continue
            all_images[key] = {
                "imageId": img_id,
                "imageName": img_name,
                "trash": True,
                "pool": pool,
                "namespace": ns,
                "parent": info.get("parent"),
                "_snapshots_raw": [],
            }

    log(f"Total images found: {len(all_images)}")
    return all_images


def resolve_snapshots_and_children(pool, all_images):
    """
    For every image, fetch its snapshots and each snapshot's children.
    Returns the set of image keys that appear as children (i.e. non-roots).
    """
    # Build lookup maps for resolving child references
    name_lookup = {}  # (pool, ns, image_name) -> key
    id_lookup = {}    # (pool, ns, image_id)   -> key

    for key, img in all_images.items():
        p, ns, img_id = key
        # For name collisions (active vs trash with same original name),
        # prefer the non-trash entry.
        name_key = (p, ns, img["imageName"])
        if name_key not in name_lookup or not img["trash"]:
            name_lookup[name_key] = key
        id_lookup[(p, ns, img_id)] = key

    child_keys = set()

    total = len(all_images)
    for idx, (key, img) in enumerate(all_images.items(), 1):
        p, ns, img_id = key
        ns_display = ns if ns else "(default)"
        log(f"  [{idx}/{total}] Snapshots for '{img['imageName']}' (id={img_id}, ns={ns_display})")

        snapshots = list_snapshots(p, ns, img_id)

        for snap in snapshots:
            snap_id = snap.get("id", "")
            snap_name = snap.get("name", "")

            children_raw = get_children(p, ns, img_id, snap_id)

            resolved = []
            for child in children_raw:
                c_pool = child.get("pool", p)
                c_ns = child.get("pool_namespace", child.get("namespace", ""))
                c_name = child.get("image", "")
                c_id = child.get("id", "")

                c_key = name_lookup.get((c_pool, c_ns, c_name))
                if not c_key and c_id:
                    c_key = id_lookup.get((c_pool, c_ns, c_id))

                if c_key:
                    child_keys.add(c_key)
                    resolved.append(c_key)
                else:
                    warn(
                        f"Could not resolve child: pool={c_pool} ns={c_ns} "
                        f"name={c_name} id={c_id}"
                    )

            img["_snapshots_raw"].append({
                "snapId": str(snap_id),
                "snapName": snap_name,
                "_children_keys": resolved,
            })

    log(f"Child (non-root) images: {len(child_keys)}")
    return child_keys


def build_node(key, all_images, is_root, pv_lookup, snap_lookup, visited=None):
    """Recursively build the output dict for one image."""
    if visited is None:
        visited = set()
    if key in visited:
        warn(f"Circular reference detected at image id {key[2]}, breaking cycle")
        return None
    visited.add(key)

    img = all_images[key]
    image_id = img["imageId"]

    node = {
        "imageId": image_id,
        "imageName": img["imageName"],
        "trash": img["trash"],
        "namespace": img["namespace"],
    }

    # Only root-level images carry the pool field
    if is_root:
        node["pool"] = img["pool"]

    # ── CSI metadata: check both lookups for every image ──
    node["pv"] = pv_lookup.get(image_id, None)
    node["snapshotContent"] = snap_lookup.get(image_id, None)

    # ── Snapshots & children ──
    node["snapshots"] = []
    for snap in img["_snapshots_raw"]:
        snap_node = {
            "snapId": snap["snapId"],
            "snapName": snap["snapName"],
            "children": [],
        }
        for child_key in snap["_children_keys"]:
            child_node = build_node(
                child_key, all_images, is_root=False,
                pv_lookup=pv_lookup, snap_lookup=snap_lookup,
                visited=visited.copy(),
            )
            if child_node:
                snap_node["children"].append(child_node)
        node["snapshots"].append(snap_node)

    return node


# ─── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Build RBD parent-child tree as JSON")
    parser.add_argument("--pool", "-p", help="RBD pool name")
    parser.add_argument("--namespace", "-n", default=None,
                        help="Specific namespace (omit to scan all)")
    parser.add_argument("-o", "--output", default=None,
                        help="Output file path (default: stdout)")
    args = parser.parse_args()

    # Interactive input if not provided via CLI
    pool = args.pool
    if not pool:
        pool = input("Enter the RBD pool name: ").strip()
    if not pool:
        print("Error: pool name is required.", file=sys.stderr)
        sys.exit(1)

    if args.namespace is not None:
        namespaces = [args.namespace]
    else:
        ns_input = input("Enter namespace (leave empty for all namespaces): ").strip()
        if ns_input:
            namespaces = [ns_input]
        else:
            namespaces = get_namespaces(pool)
            log(f"Discovered namespaces: {namespaces}")

    # Step 1 – Enumerate all images
    all_images = collect_all_images(pool, namespaces)
    if not all_images:
        warn("No images found. Nothing to do.")
        print(json.dumps({"volumes": []}, indent=2))
        sys.exit(0)

    # Step 2 – Resolve snapshot → children relationships
    child_keys = resolve_snapshots_and_children(pool, all_images)

    # Step 3 – Build CSI RADOS OMAP lookups (PV + SnapshotContent)
    pv_lookup, snap_lookup = build_csi_lookups(pool)

    # Step 4 – Identify roots (images that are NOT a child of any snapshot)
    root_keys = [k for k in all_images if k not in child_keys]
    log(f"Root images: {len(root_keys)}")

    # Step 5 – Build nested tree
    volumes = []
    for key in root_keys:
        node = build_node(key, all_images, is_root=True,
                          pv_lookup=pv_lookup, snap_lookup=snap_lookup)
        if node:
            volumes.append(node)

    result = {"volumes": volumes}

    # Output
    output_json = json.dumps(result, indent=2)
    if args.output:
        with open(args.output, "w") as f:
            f.write(output_json + "\n")
        log(f"Written to {args.output}")
    else:
        print(output_json)


if __name__ == "__main__":
    main()