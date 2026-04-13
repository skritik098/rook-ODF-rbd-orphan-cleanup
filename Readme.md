# RBD Parent-Child Tree Builder & Orphan Cleanup

A set of tools to discover, visualize, and clean up Ceph RBD image hierarchies in OpenShift Data Foundation (ODF) / Ceph CSI environments.

## Problem

In Ceph RBD pools managed by CSI (e.g., OpenShift Container Storage), PersistentVolumes and VolumeSnapshots create layered clone chains:

```
PV (root image)
 └─ snapshot
     └─ VolumeSnapshot clone (child image)
         └─ snapshot
             └─ Restored PVC clone (grandchild image)
                 └─ ...
```

When Kubernetes resources (PVCs, VolumeSnapshots) are deleted, their backing RBD images and snapshots may become orphaned — no longer referenced by any PV or VolumeSnapshotContent — but remain in the pool (often in trash) due to unresolved clone dependencies. Over time these accumulate, consuming space and blocking cleanup of parent images.

These tools automate the discovery and safe removal of such orphans.

---

## Scripts

### 1. `rbd_tree_builder.py` — Live Cluster Tree Discovery

Scans a Ceph RBD pool and builds a nested JSON tree of all images, snapshots, and clone relationships. Enriches each image with its Kubernetes CSI metadata (PV name, VolumeSnapshotContent name) by reading RADOS OMAP entries.

#### How It Works

1. **Enumerate images** — Lists all regular and trash images across specified (or all) namespaces via `rbd ls` and `rbd trash ls`.
2. **Collect metadata** — Calls `rbd info` on each image to capture the image ID and parent reference.
3. **Resolve snapshots & children** — For each image, lists snapshots (`rbd snap ls --all`) and their children (`rbd children`).
4. **Build CSI lookups** — Reads RADOS OMAP objects (`csi.volumes.default`, `csi.snaps.default`) to map RBD image IDs to PV names and VolumeSnapshotContent names.
5. **Identify roots** — Images that never appear as a child of any snapshot are root-level entries.
6. **Build tree** — Recursively nests children under their parent's snapshot, attaching CSI metadata at every level.

#### Usage

```bash
# Interactive — prompts for pool and namespace
python3 rbd_tree_builder.py

# CLI — specific namespace
python3 rbd_tree_builder.py --pool ocs-storagecluster-cephblockpool --namespace csi -o tree.json

# CLI — all namespaces
python3 rbd_tree_builder.py --pool ocs-storagecluster-cephblockpool -o tree.json
```

#### Output Structure

```json
{
  "volumes": [
    {
      "imageId": "646794c7278e",
      "imageName": "csi-vol-5a2584f6-...",
      "trash": false,
      "pool": "ocs-storagecluster-cephblockpool",
      "namespace": "",
      "pv": {
        "pvName": "pvc-458a018a-...",
        "imageName": "csi-vol-5a2584f6-...",
        "volumeOwner": "default"
      },
      "snapshotContent": null,
      "snapshots": [
        {
          "snapId": "9",
          "snapName": "00264189-...",
          "children": [
            {
              "imageId": "6467176f9340",
              "imageName": "csi-snap-de3383c4-...",
              "trash": true,
              "namespace": "",
              "pv": null,
              "snapshotContent": {
                "snapContentName": "snapshot-fb9e356c-...",
                "imageName": "csi-snap-de3383c4-...",
                "source": "csi-vol-5a2584f6-...",
                "volumeOwner": "default"
              },
              "snapshots": []
            }
          ]
        }
      ]
    }
  ]
}
```

| Field | Description |
|---|---|
| `imageId` | RBD internal image ID |
| `imageName` | RBD image name |
| `trash` | `true` if the image is in RBD trash |
| `pool` | RBD pool name (root images only) |
| `namespace` | RBD namespace (empty string = default) |
| `pv` | Kubernetes PV metadata from RADOS OMAP, or `null` |
| `snapshotContent` | Kubernetes VolumeSnapshotContent metadata, or `null` |
| `snapshots` | List of snapshots, each with its `children` |

An image with both `pv: null` and `snapshotContent: null` is considered **orphan** — no Kubernetes resource references it.

---

### 2. `rbd_tree_builder_mustGather.py` — Must-Gather Tree Discovery

Builds the same RBD parent-child-snapshot tree from **ODF must-gather data** instead of live cluster commands. Ideal for offline analysis, troubleshooting, or when direct cluster access is unavailable.

#### How It Works

1. **Auto-discover must-gather root** — Walks the provided directory to locate `ceph/must_gather_commands/`.
2. **Parse trash list** — Reads `rbd_trash_ls_<pool>` JSON files to identify trashed images.
3. **Parse image info** — Extracts image metadata (ID, parent references, snapshot count) from `rbd_vol_and_snap_info_<pool>` mixed text+JSON files.
4. **Parse snapshots** — Extracts snapshot details (ID, name, size, protected status) from the same files.
5. **Parse PV YAML files** — Reads `cluster-scoped-resources/core/persistentvolumes/*.yaml` to map RBD image names to PV names and namespaces.
6. **Build tree** — Recursively nests children under their parent's snapshot, attaching PV metadata at every level.
7. **Identify orphaned PVs** — Reports PVs whose `imageName` has no matching RBD image in the must-gather data.

#### Usage

```bash
# Basic usage — auto-discovers must-gather root
python3 rbd_tree_builder_mustGather.py /path/to/must-gather-dir

# Specify output file
python3 rbd_tree_builder_mustGather.py /path/to/must-gather-dir --output tree.json

# The must-gather-dir can be:
#   - The exact must-gather root (e.g., registry-odf4-odf-must-gather-rhel9-sha256-xxx/)
#   - A parent folder containing it (auto-discovered)
```

#### Output Structure

The output format is identical to `rbd_tree_builder.py`, with one addition:

```json
{
  "orphaned_pv": [
    {
      "pv_name": "pvc-12345678-...",
      "imageName": "csi-vol-abcdef12-...",
      "pool": "ocs-storagecluster-cephblockpool",
      "namespace": "default"
    }
  ],
  "volumes": [
    {
      "imageId": "646794c7278e",
      "imageName": "csi-vol-5a2584f6-...",
      "trash": false,
      "pool": "ocs-storagecluster-cephblockpool",
      "namespace": "",
      "pv": {
        "pvName": "pvc-458a018a-...",
        "imageName": "csi-vol-5a2584f6-...",
        "volumeOwner": "default"
      },
      "snapshots": [...]
    }
  ]
}
```

| Field | Description |
|---|---|
| `orphaned_pv` | List of PVs whose `imageName` has no matching RBD image (deleted or missing from must-gather) |
| `volumes` | Same nested tree structure as `rbd_tree_builder.py` |

**Note:** Unlike the live cluster version, this script does **not** include `snapshotContent` metadata, as VolumeSnapshotContent resources are not typically captured in must-gather data. Orphan detection relies solely on PV references.

#### Prerequisites

- Python 3.6+
- PyYAML library: `pip install pyyaml`
- ODF must-gather archive (extracted)

#### Must-Gather Structure

The script expects the following files in the must-gather:

```
must-gather-root/
├── ceph/
│   └── must_gather_commands/
│       ├── rbd_trash_ls_<pool>              # JSON: trashed images
│       └── rbd_vol_and_snap_info_<pool>     # Mixed text+JSON: image info & snapshots
└── cluster-scoped-resources/
    └── core/
        └── persistentvolumes/
            └── *.yaml                        # PV definitions
```

---

### 3. `rbd_cleanup.py` — Orphan Cleanup

Reads the JSON tree from `rbd_tree_builder.py` and removes orphan images bottom-up, handling clone dependencies along the way.

#### How It Works

1. **Pre-scan** — Counts orphan images and non-orphan children that need flattening. Prints a visual tree of the cleanup plan.
2. **Bottom-up processing** — Recursively traverses the entire tree depth. Leaf orphans are processed first, then their parents.
3. **Flatten non-orphan children** — When an orphan parent has a non-orphan child (still referenced by a PV or VolumeSnapshotContent), the tool offers to `rbd flatten` the child to break the clone dependency before deleting the parent.
4. **Trash restore** — If an orphan parent is in trash, it is restored via `rbd trash restore` before flattening (flatten requires the parent to be accessible), then deleted after cleanup.
5. **Auto-purge detection** — Before deleting snapshots or images, the tool checks if they still exist on the cluster. Ceph may auto-purge trash images or snapshots once their last dependent is removed — the tool handles this gracefully.
6. **Interactive confirmation** — Each deletion and flatten is confirmed individually with `y/n`.

#### Usage

```bash
# Dry run — shows what would happen without executing anything
python3 rbd_cleanup.py --dry-run tree.json

# Interactive cleanup
python3 rbd_cleanup.py tree.json
```

#### Example Dry-Run Output

```
======================================================================
  Cleanup Plan
  Orphan images to delete        : 3
  Non-orphan children to flatten  : 2
======================================================================

✗ csi-vol-5a2584f6-... [TRASH]  (id=646794c7278e)  ← ORPHAN (delete)
  └─ snap: 00264189-...  (snap-id: 9)
      ✗ csi-snap-de3383c4-... [TRASH]  (id=6467176f9340)  ← ORPHAN (delete)
        └─ snap: ef2de7e3-...  (snap-id: 10)
            ✓ csi-vol-eda96d9d-...  (id=6467cf5a3df2)  ← NON-ORPHAN (flatten) ref=pvc-b43dc667-...
```

#### Deletion Order (per orphan image)

1. Check if the image still exists (may have been auto-purged)
2. Query live snapshots from the cluster
3. For each snapshot that still exists: unprotect → remove
4. Remove the image (`rbd rm` or `rbd trash remove`)

---

## Prerequisites

- Python 3.6+
- Access to `rbd` and `rados` CLI tools (run from a Ceph node or pod with admin credentials)
- Sufficient permissions to run `rbd info`, `rbd snap ls`, `rbd children`, `rbd flatten`, `rbd trash restore`, `rbd rm`, and `rados` OMAP commands

## Workflows

### Live Cluster Workflow

```bash
# Step 1: Generate the tree from live cluster
python3 rbd_tree_builder.py --pool <pool-name> -o tree.json

# Step 2: Review the tree
cat tree.json | python3 -m json.tool

# Step 3: Dry-run cleanup
python3 rbd_cleanup.py --dry-run tree.json

# Step 4: Execute cleanup
python3 rbd_cleanup.py tree.json
```

### Must-Gather Workflow

```bash
# Step 1: Generate the tree from must-gather
python3 rbd_tree_builder_mustGather.py /path/to/must-gather -o tree.json

# Step 2: Review the tree and orphaned PVs
cat tree.json | python3 -m json.tool

# Step 3: Analyze orphans offline
# (The tree can be used for analysis; cleanup requires live cluster access)
```

> **Note:** Always regenerate `tree.json` before running cleanup if the pool state may have changed since the last scan. The cleanup script operates on the tree snapshot, not live cluster state (except for existence checks).

---

## Limitations

- **Single pool only** — cross-pool clone chains are not supported.
- **RADOS OMAP objects** — CSI metadata lookup assumes the standard `csi.volumes.default` / `csi.snaps.default` OMAP objects exist in the default RADOS namespace.
- **Flatten duration** — `rbd flatten` on large images can take significant time. The command timeout is set to 300 seconds; adjust if needed.
- **Concurrent modifications** — If other processes are creating/deleting images during a run, results may be inconsistent. Run during a maintenance window if possible.