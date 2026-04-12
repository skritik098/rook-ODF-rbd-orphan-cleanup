#!/usr/bin/env python3
"""
ODF Orphan Resource Finder
===========================
Identifies orphan RBD images, CephFS subvolumes, snapshots, and their
hierarchical relationships in ODF / Rook-Ceph OCP environments.

Supports two execution modes:
  --live          : Runs commands on a live cluster via oc exec into rook-ceph-tools pod
  --must-gather   : Parses an ODF must-gather directory (offline analysis)

Usage:
  python3 odf_orphan_finder.py --live [--namespace openshift-storage]
  python3 odf_orphan_finder.py --must-gather /path/to/must-gather
  python3 odf_orphan_finder.py --live --output report.txt --json

Author : Kriitk (IBM Storage Support)
License: Internal / Apache-2.0
"""

import argparse
import json
import subprocess
import sys
import os
import glob
import re
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple, Any
from pathlib import Path
from collections import defaultdict
from datetime import datetime

# ════════════════════════════════════════════════════════════════
# ANSI Colors for terminal output
# ════════════════════════════════════════════════════════════════

class C:
    """Terminal colors"""
    RED     = "\033[91m"
    GREEN   = "\033[92m"
    YELLOW  = "\033[93m"
    BLUE    = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN    = "\033[96m"
    BOLD    = "\033[1m"
    DIM     = "\033[2m"
    RESET   = "\033[0m"

    @staticmethod
    def disable():
        for attr in ['RED','GREEN','YELLOW','BLUE','MAGENTA','CYAN','BOLD','DIM','RESET']:
            setattr(C, attr, '')


# ════════════════════════════════════════════════════════════════
# Data Models
# ════════════════════════════════════════════════════════════════

@dataclass
class RBDSnapshot:
    name: str
    snap_id: int = 0
    size: int = 0
    protected: bool = False

@dataclass
class RBDImage:
    pool: str
    name: str
    image_id: str = ""
    size: int = 0
    parent_pool: str = ""
    parent_image: str = ""
    parent_snap: str = ""
    snapshots: List[RBDSnapshot] = field(default_factory=list)
    features: List[str] = field(default_factory=list)

@dataclass
class RBDTrashEntry:
    pool: str
    trash_id: str
    original_name: str
    # info fetched via rbd info -p <pool> --image-id <trash_id>
    size: int = 0
    parent_pool: str = ""
    parent_image: str = ""
    parent_snap: str = ""
    snapshots: List[RBDSnapshot] = field(default_factory=list)

@dataclass
class CephFSSubvolume:
    fs_name: str
    group: str
    name: str
    vol_type: str = ""          # "subvolume" or "clone"
    path: str = ""
    snapshots: List[str] = field(default_factory=list)

@dataclass
class K8sPV:
    name: str
    status: str = ""
    capacity: str = ""
    storage_class: str = ""
    claim_ref: str = ""         # namespace/pvc-name
    driver: str = ""            # rbd or cephfs CSI driver
    # RBD fields
    image_name: str = ""
    pool: str = ""
    # CephFS fields
    subvolume_name: str = ""
    fs_name: str = ""

@dataclass
class K8sVolumeSnapshot:
    name: str
    namespace: str
    vsc_name: str = ""
    source_pvc: str = ""
    ready: bool = False

@dataclass
class K8sVSC:
    name: str
    snapshot_handle: str = ""
    driver: str = ""
    vs_name: str = ""
    vs_namespace: str = ""
    deletion_policy: str = ""


# ════════════════════════════════════════════════════════════════
# Command Runners
# ════════════════════════════════════════════════════════════════

class LiveRunner:
    """Execute commands on a live OCP cluster via oc exec."""

    def __init__(self, namespace: str = "openshift-storage"):
        self.namespace = namespace
        self.tools_pod = None
        self.tools_ns = None
        self._discover_tools_pod()

    def _discover_tools_pod(self):
        """Find the rook-ceph-tools pod."""
        print(f"{C.CYAN}[CMD]{C.RESET} Discovering rook-ceph-tools pod...")
        try:
            result = subprocess.run(
                ["oc", "get", "pod", "-A",
                 "-l", "app=rook-ceph-tools",
                 "--field-selector=status.phase=Running",
                 "-o", "jsonpath={.items[0].metadata.name}|{.items[0].metadata.namespace}"],
                capture_output=True, text=True, timeout=30
            )
            if result.returncode != 0 or not result.stdout.strip():
                print(f"{C.RED}ERROR: Could not find running rook-ceph-tools pod.{C.RESET}")
                print(f"  Make sure the tools pod is deployed: oc patch OCSInitialization ocsinit -n {self.namespace} "
                      f"--type json --patch '[{{ \"op\": \"replace\", \"path\": \"/spec/enableCephTools\", \"value\": true }}]'")
                sys.exit(1)
            parts = result.stdout.strip().split("|")
            self.tools_pod = parts[0]
            self.tools_ns = parts[1] if len(parts) > 1 else self.namespace
            print(f"{C.GREEN}  Tools pod: {self.tools_pod} (ns: {self.tools_ns}){C.RESET}")
        except FileNotFoundError:
            print(f"{C.RED}ERROR: 'oc' command not found. Make sure you are logged into the cluster.{C.RESET}")
            sys.exit(1)
        except subprocess.TimeoutExpired:
            print(f"{C.RED}ERROR: Timed out looking for tools pod.{C.RESET}")
            sys.exit(1)

    def run_ceph(self, cmd: str, timeout: int = 60) -> str:
        """Run a ceph/rbd command inside the tools pod."""
        full_cmd = ["oc", "exec", "-n", self.tools_ns, self.tools_pod, "--", "bash", "-c", cmd]
        print(f"{C.DIM}  [CMD] oc exec ... -- {cmd}{C.RESET}")
        try:
            result = subprocess.run(full_cmd, capture_output=True, text=True, timeout=timeout)
            if result.returncode != 0:
                # Some commands return non-zero for empty results, handle gracefully
                if "No such file" in result.stderr or "does not exist" in result.stderr:
                    return ""
                # Print stderr but don't fail for warnings
                if result.stderr.strip():
                    print(f"{C.DIM}    stderr: {result.stderr.strip()[:200]}{C.RESET}")
            return result.stdout.strip()
        except subprocess.TimeoutExpired:
            print(f"{C.YELLOW}  WARN: Command timed out: {cmd[:80]}{C.RESET}")
            return ""

    def run_oc(self, cmd: str, timeout: int = 60) -> str:
        """Run an oc command."""
        full_cmd = ["oc"] + cmd.split()
        print(f"{C.DIM}  [CMD] oc {cmd}{C.RESET}")
        try:
            result = subprocess.run(full_cmd, capture_output=True, text=True, timeout=timeout)
            return result.stdout.strip()
        except subprocess.TimeoutExpired:
            print(f"{C.YELLOW}  WARN: Command timed out: oc {cmd[:80]}{C.RESET}")
            return ""


class MustGatherRunner:
    """Parse an ODF must-gather directory for offline analysis."""

    def __init__(self, mg_path: str):
        self.mg_root = Path(mg_path)
        if not self.mg_root.exists():
            print(f"{C.RED}ERROR: Must-gather path does not exist: {mg_path}{C.RESET}")
            sys.exit(1)
        self._scan_structure()

    def _scan_structure(self):
        """Identify the must-gather internal structure."""
        print(f"{C.CYAN}[INFO]{C.RESET} Scanning must-gather: {self.mg_root}")
        # Find the root of actual content (often nested under image path)
        # Look for a 'ceph' directory as anchor
        ceph_dirs = list(self.mg_root.rglob("ceph"))
        if ceph_dirs:
            # Use the first found ceph dir, go up one level for the content root
            self.content_root = ceph_dirs[0].parent
            self.ceph_dir = ceph_dirs[0]
            print(f"{C.GREEN}  Content root: {self.content_root}{C.RESET}")
            print(f"{C.GREEN}  Ceph dir: {self.ceph_dir}{C.RESET}")
        else:
            self.content_root = self.mg_root
            self.ceph_dir = None
            print(f"{C.YELLOW}  WARN: No 'ceph' directory found; some data may be missing.{C.RESET}")

    def _find_file(self, patterns: List[str]) -> Optional[Path]:
        """Find a file matching any of the given glob patterns."""
        search_root = self.ceph_dir if self.ceph_dir else self.content_root
        for pattern in patterns:
            matches = list(search_root.rglob(pattern))
            if matches:
                return matches[0]
        # Also search content_root
        for pattern in patterns:
            matches = list(self.content_root.rglob(pattern))
            if matches:
                return matches[0]
        return None

    def _find_files(self, patterns: List[str]) -> List[Path]:
        """Find all files matching any of the given glob patterns."""
        results = []
        search_root = self.content_root
        for pattern in patterns:
            results.extend(search_root.rglob(pattern))
        return results

    def _read_file(self, path: Path) -> str:
        """Read file contents."""
        try:
            return path.read_text(errors='replace').strip()
        except Exception as e:
            print(f"{C.YELLOW}  WARN: Could not read {path}: {e}{C.RESET}")
            return ""

    def _parse_json_safe(self, text: str) -> Any:
        """Safely parse JSON, return None on failure."""
        try:
            return json.loads(text)
        except (json.JSONDecodeError, TypeError):
            return None


# ════════════════════════════════════════════════════════════════
# Data Collector
# ════════════════════════════════════════════════════════════════

class DataCollector:
    """Collects all Ceph and K8s data from either a live cluster or must-gather."""

    def __init__(self, runner):
        self.runner = runner
        self.is_live = isinstance(runner, LiveRunner)

        # Collected data
        self.rbd_pools: List[str] = []
        self.rbd_images: Dict[str, List[RBDImage]] = {}          # pool -> images
        self.rbd_trash: Dict[str, List[RBDTrashEntry]] = {}      # pool -> trash entries
        self.cephfs_name: str = ""
        self.cephfs_subvolumes: List[CephFSSubvolume] = []
        self.pvs: List[K8sPV] = []
        self.volume_snapshots: List[K8sVolumeSnapshot] = []
        self.vscs: List[K8sVSC] = []

    def collect_all(self):
        """Run full data collection."""
        print(f"\n{C.BOLD}{'='*60}{C.RESET}")
        print(f"{C.BOLD}  DATA COLLECTION{C.RESET}")
        print(f"{C.BOLD}{'='*60}{C.RESET}\n")

        if self.is_live:
            self._collect_live()
        else:
            self._collect_must_gather()

    # ──────────────────────────────────────────
    # LIVE CLUSTER COLLECTION
    # ──────────────────────────────────────────

    def _collect_live(self):
        r = self.runner

        # 1. RBD Pools
        print(f"{C.CYAN}[1/8]{C.RESET} Collecting RBD pools...")
        raw = r.run_ceph("ceph osd pool ls detail --format json")
        pools_data = json.loads(raw) if raw else []
        self.rbd_pools = [
            p['pool_name'] for p in pools_data
            if 'rbd' in p.get('application_metadata', {})
        ]
        print(f"  Found {len(self.rbd_pools)} RBD pool(s): {', '.join(self.rbd_pools)}")

        # 2. RBD Images + Info + Snapshots
        print(f"\n{C.CYAN}[2/8]{C.RESET} Collecting RBD images, info, and snapshots...")
        for pool in self.rbd_pools:
            raw = r.run_ceph(f"rbd ls --pool {pool} --format json")
            image_names = json.loads(raw) if raw else []
            images = []
            for img_name in image_names:
                img = self._fetch_live_rbd_info(pool, img_name)
                if img:
                    img.snapshots = self._fetch_live_rbd_snaps(pool, img_name)
                    images.append(img)
            self.rbd_images[pool] = images
            print(f"  Pool '{pool}': {len(images)} image(s)")

        # 3. RBD Trash
        print(f"\n{C.CYAN}[3/8]{C.RESET} Collecting RBD trash entries...")
        for pool in self.rbd_pools:
            raw = r.run_ceph(f"rbd trash ls --pool {pool} --format json")
            trash_data = json.loads(raw) if raw else []
            entries = []
            for t in trash_data:
                entry = RBDTrashEntry(
                    pool=pool,
                    trash_id=t.get('id', ''),
                    original_name=t.get('name', ''),
                )
                # Get detailed info from trash using --image-id
                info_raw = r.run_ceph(f"rbd info -p {pool} --image-id {entry.trash_id} --format json")
                if info_raw:
                    info = json.loads(info_raw)
                    entry.size = info.get('size', 0)
                    parent = info.get('parent', {})
                    if parent:
                        entry.parent_pool = parent.get('pool', '')
                        entry.parent_image = parent.get('image', '')
                        entry.parent_snap = parent.get('snapshot', '')
                    # Snapshots on trashed image
                    snap_raw = r.run_ceph(
                        f"rbd snap ls -p {pool} --image-id {entry.trash_id} --format json"
                    )
                    if snap_raw:
                        for s in json.loads(snap_raw):
                            entry.snapshots.append(RBDSnapshot(
                                name=s.get('name', ''),
                                snap_id=s.get('id', 0),
                                size=s.get('size', 0),
                                protected=s.get('protected', 'false') == 'true'
                            ))
                entries.append(entry)
            self.rbd_trash[pool] = entries
            print(f"  Pool '{pool}': {len(entries)} trash entry(ies)")

        # 4. CephFS filesystem name
        print(f"\n{C.CYAN}[4/8]{C.RESET} Collecting CephFS info...")
        raw = r.run_ceph("ceph fs ls --format json")
        fs_list = json.loads(raw) if raw else []
        if fs_list:
            self.cephfs_name = fs_list[0].get('name', '')
            print(f"  Filesystem: {self.cephfs_name}")
        else:
            print(f"  {C.YELLOW}No CephFS filesystem found.{C.RESET}")

        # 5. CephFS Subvolumes + Info + Snapshots
        if self.cephfs_name:
            print(f"\n{C.CYAN}[5/8]{C.RESET} Collecting CephFS subvolumes, info, and snapshots...")
            raw = r.run_ceph(
                f"ceph fs subvolume ls {self.cephfs_name} --group_name csi --format json"
            )
            subvol_list = json.loads(raw) if raw else []
            for sv in subvol_list:
                name = sv.get('name', '')
                subvol = CephFSSubvolume(
                    fs_name=self.cephfs_name,
                    group="csi",
                    name=name,
                )
                # Get subvolume info (type field)
                info_raw = r.run_ceph(
                    f"ceph fs subvolume info {self.cephfs_name} {name} csi --format json"
                )
                if info_raw:
                    info = json.loads(info_raw)
                    subvol.vol_type = info.get('type', 'subvolume')
                    subvol.path = info.get('path', '')

                # Get subvolume snapshots
                snap_raw = r.run_ceph(
                    f"ceph fs subvolume snapshot ls {self.cephfs_name} {name} csi --format json 2>/dev/null"
                )
                if snap_raw:
                    try:
                        for sn in json.loads(snap_raw):
                            subvol.snapshots.append(sn.get('name', ''))
                    except json.JSONDecodeError:
                        pass
                self.cephfs_subvolumes.append(subvol)
            print(f"  Found {len(self.cephfs_subvolumes)} subvolume(s)")
        else:
            print(f"\n{C.CYAN}[5/8]{C.RESET} Skipping CephFS (no filesystem).")

        # 6. K8s PVs
        print(f"\n{C.CYAN}[6/8]{C.RESET} Collecting PersistentVolumes...")
        self.pvs = self._collect_live_pvs()
        rbd_pvs = [p for p in self.pvs if p.image_name]
        cephfs_pvs = [p for p in self.pvs if p.subvolume_name]
        print(f"  Found {len(self.pvs)} PV(s) — {len(rbd_pvs)} RBD, {len(cephfs_pvs)} CephFS")

        # 7. K8s VolumeSnapshotContents
        print(f"\n{C.CYAN}[7/8]{C.RESET} Collecting VolumeSnapshotContents...")
        self.vscs = self._collect_live_vscs()
        print(f"  Found {len(self.vscs)} VSC(s)")

        # 8. K8s VolumeSnapshots
        print(f"\n{C.CYAN}[8/8]{C.RESET} Collecting VolumeSnapshots...")
        self.volume_snapshots = self._collect_live_vs()
        print(f"  Found {len(self.volume_snapshots)} VolumeSnapshot(s)")

    def _fetch_live_rbd_info(self, pool: str, image: str) -> Optional[RBDImage]:
        raw = self.runner.run_ceph(f"rbd info {pool}/{image} --format json")
        if not raw:
            return RBDImage(pool=pool, name=image)
        try:
            info = json.loads(raw)
        except json.JSONDecodeError:
            return RBDImage(pool=pool, name=image)
        img = RBDImage(
            pool=pool,
            name=image,
            image_id=info.get('id', ''),
            size=info.get('size', 0),
            features=info.get('features', []) if isinstance(info.get('features'), list)
                     else [info.get('features', '')],
        )
        parent = info.get('parent', {})
        if parent:
            img.parent_pool = parent.get('pool', '')
            img.parent_image = parent.get('image', '')
            img.parent_snap = parent.get('snapshot', '')
        return img

    def _fetch_live_rbd_snaps(self, pool: str, image: str) -> List[RBDSnapshot]:
        raw = self.runner.run_ceph(f"rbd snap ls {pool}/{image} --format json")
        if not raw:
            return []
        try:
            snaps = json.loads(raw)
        except json.JSONDecodeError:
            return []
        result = []
        for s in snaps:
            result.append(RBDSnapshot(
                name=s.get('name', ''),
                snap_id=s.get('id', 0),
                size=s.get('size', 0),
                protected=str(s.get('protected', 'false')).lower() == 'true'
            ))
        return result

    def _collect_live_pvs(self) -> List[K8sPV]:
        raw = self.runner.run_oc("get pv -o json")
        if not raw:
            return []
        data = json.loads(raw)
        return self._parse_pvs_json(data)

    def _collect_live_vscs(self) -> List[K8sVSC]:
        raw = self.runner.run_oc("get volumesnapshotcontent -o json")
        if not raw:
            return []
        data = json.loads(raw)
        return self._parse_vscs_json(data)

    def _collect_live_vs(self) -> List[K8sVolumeSnapshot]:
        raw = self.runner.run_oc("get volumesnapshot -A -o json")
        if not raw:
            return []
        data = json.loads(raw)
        return self._parse_vs_json(data)

    # ──────────────────────────────────────────
    # MUST-GATHER COLLECTION
    # ──────────────────────────────────────────

    def _collect_must_gather(self):
        r = self.runner

        # 1. RBD Pools — discover from multiple sources
        print(f"{C.CYAN}[1/8]{C.RESET} Parsing RBD pools from must-gather...")

        # Strategy A: JSON pool list (ceph osd pool ls detail --format json)
        pool_file = r._find_file([
            "*ceph_osd_pool_ls*json*",
            "*pool_ls_detail*json*",
            "*osd_pool_ls*",
        ])
        if pool_file:
            raw = r._read_file(pool_file)
            pools_data = r._parse_json_safe(raw) or []
            self.rbd_pools = [
                p['pool_name'] for p in pools_data
                if isinstance(p, dict) and 'rbd' in p.get('application_metadata', {})
            ]

        # Strategy B: infer pools from rbd_vol_and_snap_info_<pool> filenames
        if not self.rbd_pools:
            vol_snap_files = r._find_files([
                "rbd_vol_and_snap_info_*",
            ])
            for f in vol_snap_files:
                # filename: rbd_vol_and_snap_info_<pool>
                fname = f.name
                prefix = "rbd_vol_and_snap_info_"
                if fname.startswith(prefix):
                    pool = fname[len(prefix):]
                    if pool and pool not in self.rbd_pools:
                        self.rbd_pools.append(pool)

        # Strategy C: infer from rbd_trash_ls_<pool> filenames
        if not self.rbd_pools:
            trash_files = r._find_files(["rbd_trash_ls_*"])
            for f in trash_files:
                fname = f.name
                prefix = "rbd_trash_ls_"
                if fname.startswith(prefix):
                    pool = fname[len(prefix):]
                    if pool and pool not in self.rbd_pools:
                        self.rbd_pools.append(pool)

        print(f"  Found {len(self.rbd_pools)} RBD pool(s): {', '.join(self.rbd_pools)}")

        # 2. RBD Images + Info + Snapshots — parse the consolidated rbd_vol_and_snap_info file
        print(f"\n{C.CYAN}[2/8]{C.RESET} Parsing RBD images from must-gather...")
        for pool in self.rbd_pools:
            images = []

            # Primary: parse rbd_vol_and_snap_info_<pool> (consolidated text file)
            vol_snap_file = r._find_file([
                f"rbd_vol_and_snap_info_{pool}",
                f"*rbd_vol_and_snap_info_{pool}",
                f"*rbd_vol_and_snap_info*{pool}*",
            ])
            if vol_snap_file:
                raw = r._read_file(vol_snap_file)
                images = self._parse_rbd_vol_and_snap_info(pool, raw)
                print(f"  Pool '{pool}': parsed rbd_vol_and_snap_info → {len(images)} image(s)")
            else:
                # Fallback: try individual rbd_ls / rbd_info files (older must-gather format)
                ls_file = r._find_file([
                    f"*rbd_ls_{pool}*json*",
                    f"*rbd_ls_--pool_{pool}*json*",
                    f"*rbd_ls*{pool}*",
                ])
                if ls_file:
                    raw = r._read_file(ls_file)
                    parsed = r._parse_json_safe(raw)
                    image_names = parsed if isinstance(parsed, list) else []
                    for img_name in image_names:
                        img = RBDImage(pool=pool, name=img_name)
                        info_file = r._find_file([
                            f"*rbd_info_{pool}_{img_name}*",
                            f"*rbd_info*{pool}*{img_name}*",
                        ])
                        if info_file:
                            raw = r._read_file(info_file)
                            info = r._parse_json_safe(raw)
                            if not info:
                                info = self._parse_rbd_info_text(raw)
                            if info:
                                img.image_id = info.get('id', '')
                                img.size = info.get('size', 0)
                                parent = info.get('parent', {})
                                if parent:
                                    img.parent_pool = parent.get('pool', '')
                                    img.parent_image = parent.get('image', '')
                                    img.parent_snap = parent.get('snapshot', '')
                        images.append(img)
                print(f"  Pool '{pool}': fallback parsing → {len(images)} image(s)")

            self.rbd_images[pool] = images

        # 3. RBD Trash — parse rbd_trash_ls_<pool> (plain text or JSON)
        print(f"\n{C.CYAN}[3/8]{C.RESET} Parsing RBD trash from must-gather...")
        for pool in self.rbd_pools:
            trash_file = r._find_file([
                f"rbd_trash_ls_{pool}",
                f"*rbd_trash_ls_{pool}",
                f"*rbd_trash_ls*{pool}*",
                f"*trash_ls_{pool}*",
            ])
            entries = []
            if trash_file:
                raw = r._read_file(trash_file)
                # Try JSON first
                trash_data = r._parse_json_safe(raw)
                if isinstance(trash_data, list):
                    for t in trash_data:
                        if isinstance(t, dict):
                            entries.append(RBDTrashEntry(
                                pool=pool,
                                trash_id=t.get('id', ''),
                                original_name=t.get('name', ''),
                            ))
                else:
                    # Plain text format: parse line by line
                    entries = self._parse_rbd_trash_text(pool, raw)

                # Try to enrich trash entries with info from rbd_vol_and_snap_info if available
                # (the consolidated file may include trash image info too)
                # Also try dedicated trash info files
                for entry in entries:
                    trash_info_file = r._find_file([
                        f"*rbd_info*{pool}*{entry.trash_id}*",
                        f"*image-id*{entry.trash_id}*",
                    ])
                    if trash_info_file:
                        info_raw = r._read_file(trash_info_file)
                        info = r._parse_json_safe(info_raw)
                        if not info:
                            info = self._parse_rbd_info_text(info_raw)
                        if info:
                            entry.size = info.get('size', 0)
                            parent = info.get('parent', {})
                            if parent:
                                entry.parent_pool = parent.get('pool', '')
                                entry.parent_image = parent.get('image', '')
                                entry.parent_snap = parent.get('snapshot', '')

            self.rbd_trash[pool] = entries
            print(f"  Pool '{pool}': {len(entries)} trash entry(ies)")

        # 4. CephFS filesystem name
        print(f"\n{C.CYAN}[4/8]{C.RESET} Parsing CephFS info from must-gather...")
        fs_file = r._find_file([
            "*ceph_fs_ls*json*",
            "*fs_ls*json*",
        ])
        if fs_file:
            raw = r._read_file(fs_file)
            fs_list = r._parse_json_safe(raw) or []
            if fs_list and isinstance(fs_list[0], dict):
                self.cephfs_name = fs_list[0].get('name', '')
                print(f"  Filesystem: {self.cephfs_name}")
        if not self.cephfs_name:
            print(f"  {C.YELLOW}No CephFS filesystem found in must-gather.{C.RESET}")

        # 5. CephFS Subvolumes
        if self.cephfs_name:
            print(f"\n{C.CYAN}[5/8]{C.RESET} Parsing CephFS subvolumes from must-gather...")
            sv_file = r._find_file([
                f"*subvolume_ls*{self.cephfs_name}*json*",
                f"*subvolume_ls*csi*json*",
                "*subvolume_ls*",
            ])
            if sv_file:
                raw = r._read_file(sv_file)
                sv_list = r._parse_json_safe(raw) or []
                for sv in sv_list:
                    if isinstance(sv, dict):
                        name = sv.get('name', '')
                        subvol = CephFSSubvolume(
                            fs_name=self.cephfs_name,
                            group="csi",
                            name=name,
                        )
                        # Try to find subvolume info
                        info_file = r._find_file([
                            f"*subvolume_info*{name}*",
                        ])
                        if info_file:
                            raw = r._read_file(info_file)
                            info = r._parse_json_safe(raw) or {}
                            subvol.vol_type = info.get('type', 'subvolume')
                            subvol.path = info.get('path', '')

                        # Try to find subvolume snapshots
                        snap_file = r._find_file([
                            f"*subvolume_snapshot_ls*{name}*json*",
                            f"*snapshot_ls*{name}*",
                        ])
                        if snap_file:
                            raw = r._read_file(snap_file)
                            snaps = r._parse_json_safe(raw) or []
                            for sn in snaps:
                                if isinstance(sn, dict):
                                    subvol.snapshots.append(sn.get('name', ''))

                        self.cephfs_subvolumes.append(subvol)
            print(f"  Found {len(self.cephfs_subvolumes)} subvolume(s)")
        else:
            print(f"\n{C.CYAN}[5/8]{C.RESET} Skipping CephFS (no filesystem).")

        # 6-8. K8s resources from must-gather
        print(f"\n{C.CYAN}[6/8]{C.RESET} Parsing PersistentVolumes from must-gather...")
        self.pvs = self._collect_mg_pvs()
        rbd_pvs = [p for p in self.pvs if p.image_name]
        cephfs_pvs = [p for p in self.pvs if p.subvolume_name]
        print(f"  Found {len(self.pvs)} PV(s) — {len(rbd_pvs)} RBD, {len(cephfs_pvs)} CephFS")

        print(f"\n{C.CYAN}[7/8]{C.RESET} Parsing VolumeSnapshotContents from must-gather...")
        self.vscs = self._collect_mg_vscs()
        print(f"  Found {len(self.vscs)} VSC(s)")

        print(f"\n{C.CYAN}[8/8]{C.RESET} Parsing VolumeSnapshots from must-gather...")
        self.volume_snapshots = self._collect_mg_vs()
        print(f"  Found {len(self.volume_snapshots)} VolumeSnapshot(s)")

    def _collect_mg_pvs(self) -> List[K8sPV]:
        r = self.runner
        pvs = []
        # Try consolidated JSON file first
        pv_json = r._find_file(["*persistentvolumes.json", "*persistentvolumes*.json"])
        if pv_json:
            raw = r._read_file(pv_json)
            data = r._parse_json_safe(raw)
            if data:
                return self._parse_pvs_json(data)

        # Fall back to individual YAML/JSON files
        pv_files = r._find_files([
            "*/persistentvolumes/*.yaml",
            "*/persistentvolumes/*.json",
            "*/core/persistentvolumes/*.yaml",
        ])
        for f in pv_files:
            raw = r._read_file(f)
            if raw.startswith('{'):
                data = r._parse_json_safe(raw)
                if data:
                    pv = self._parse_single_pv(data)
                    if pv:
                        pvs.append(pv)
            else:
                # YAML — parse manually for key fields
                pv = self._parse_pv_yaml(raw)
                if pv:
                    pvs.append(pv)
        return pvs

    def _collect_mg_vscs(self) -> List[K8sVSC]:
        r = self.runner
        vscs = []
        vsc_files = r._find_files([
            "*/volumesnapshotcontents/*.yaml",
            "*/volumesnapshotcontents/*.json",
        ])
        for f in vsc_files:
            raw = r._read_file(f)
            if raw.startswith('{'):
                data = r._parse_json_safe(raw)
                if data:
                    vsc = self._parse_single_vsc(data)
                    if vsc:
                        vscs.append(vsc)
            else:
                vsc = self._parse_vsc_yaml(raw)
                if vsc:
                    vscs.append(vsc)
        return vscs

    def _collect_mg_vs(self) -> List[K8sVolumeSnapshot]:
        r = self.runner
        vss = []
        vs_files = r._find_files([
            "*/volumesnapshots/*.yaml",
            "*/volumesnapshots/*.json",
            "*volumesnapshots.yaml",
        ])
        for f in vs_files:
            raw = r._read_file(f)
            data = r._parse_json_safe(raw)
            if data:
                if 'items' in data:
                    for item in data['items']:
                        vs = self._parse_single_vs(item)
                        if vs:
                            vss.append(vs)
                elif 'metadata' in data:
                    vs = self._parse_single_vs(data)
                    if vs:
                        vss.append(vs)
        return vss

    # ──────────────────────────────────────────
    # JSON Parsers (shared between modes)
    # ──────────────────────────────────────────

    def _parse_pvs_json(self, data: dict) -> List[K8sPV]:
        pvs = []
        items = data.get('items', []) if isinstance(data, dict) else []
        for item in items:
            pv = self._parse_single_pv(item)
            if pv:
                pvs.append(pv)
        return pvs

    def _parse_single_pv(self, item: dict) -> Optional[K8sPV]:
        meta = item.get('metadata', {})
        spec = item.get('spec', {})
        status = item.get('status', {})
        csi = spec.get('csi', {})
        vol_attrs = csi.get('volumeAttributes', {})
        driver = csi.get('driver', '')

        # Only care about Ceph CSI PVs
        if 'rbd.csi.ceph.com' not in driver and 'cephfs.csi.ceph.com' not in driver:
            return None

        claim_ref = spec.get('claimRef', {})
        claim_str = ""
        if claim_ref:
            claim_str = f"{claim_ref.get('namespace', '')}/{claim_ref.get('name', '')}"

        capacity = spec.get('capacity', {}).get('storage', '')

        pv = K8sPV(
            name=meta.get('name', ''),
            status=status.get('phase', ''),
            capacity=capacity,
            storage_class=spec.get('storageClassName', ''),
            claim_ref=claim_str,
            driver=driver,
        )
        if 'rbd.csi.ceph.com' in driver:
            pv.image_name = vol_attrs.get('imageName', '')
            pv.pool = vol_attrs.get('pool', '')
        elif 'cephfs.csi.ceph.com' in driver:
            pv.subvolume_name = vol_attrs.get('subvolumeName', '')
            pv.fs_name = vol_attrs.get('fsName', '')
        return pv

    def _parse_vscs_json(self, data: dict) -> List[K8sVSC]:
        vscs = []
        items = data.get('items', []) if isinstance(data, dict) else []
        for item in items:
            vsc = self._parse_single_vsc(item)
            if vsc:
                vscs.append(vsc)
        return vscs

    def _parse_single_vsc(self, item: dict) -> Optional[K8sVSC]:
        meta = item.get('metadata', {})
        spec = item.get('spec', {})
        status = item.get('status', {})
        vs_ref = spec.get('volumeSnapshotRef', {})
        return K8sVSC(
            name=meta.get('name', ''),
            snapshot_handle=status.get('snapshotHandle', '') or spec.get('source', {}).get('snapshotHandle', ''),
            driver=spec.get('driver', ''),
            vs_name=vs_ref.get('name', ''),
            vs_namespace=vs_ref.get('namespace', ''),
            deletion_policy=spec.get('deletionPolicy', ''),
        )

    def _parse_vs_json(self, data: dict) -> List[K8sVolumeSnapshot]:
        vss = []
        items = data.get('items', []) if isinstance(data, dict) else []
        for item in items:
            vs = self._parse_single_vs(item)
            if vs:
                vss.append(vs)
        return vss

    def _parse_single_vs(self, item: dict) -> Optional[K8sVolumeSnapshot]:
        meta = item.get('metadata', {})
        spec = item.get('spec', {})
        status = item.get('status', {})
        source = spec.get('source', {})
        return K8sVolumeSnapshot(
            name=meta.get('name', ''),
            namespace=meta.get('namespace', ''),
            vsc_name=status.get('boundVolumeSnapshotContentName', '') or
                     spec.get('source', {}).get('volumeSnapshotContentName', ''),
            source_pvc=source.get('persistentVolumeClaimName', ''),
            ready=bool(status.get('readyToUse', False)),
        )

    # ──────────────────────────────────────────
    # YAML fallback parsers (for must-gather)
    # ──────────────────────────────────────────

    def _parse_pv_yaml(self, raw: str) -> Optional[K8sPV]:
        """Quick regex-based PV parser for must-gather YAML files."""
        # Only for Ceph CSI PVs
        if 'rbd.csi.ceph.com' not in raw and 'cephfs.csi.ceph.com' not in raw:
            return None
        name = self._yaml_val(raw, 'name', top_level=True)
        pv = K8sPV(
            name=name or "",
            status=self._yaml_val(raw, 'phase') or "",
            capacity=self._yaml_val(raw, 'storage') or "",
            storage_class=self._yaml_val(raw, 'storageClassName') or "",
            driver=self._yaml_val(raw, 'driver') or "",
        )
        if 'rbd.csi.ceph.com' in raw:
            pv.image_name = self._yaml_val(raw, 'imageName') or ""
            pv.pool = self._yaml_val(raw, 'pool') or ""
        elif 'cephfs.csi.ceph.com' in raw:
            pv.subvolume_name = self._yaml_val(raw, 'subvolumeName') or ""
            pv.fs_name = self._yaml_val(raw, 'fsName') or ""

        # claimRef
        ns = self._yaml_val(raw, 'namespace')
        pvc_name_match = re.search(r'claimRef:.*?name:\s*(\S+)', raw, re.DOTALL)
        if pvc_name_match and ns:
            pv.claim_ref = f"{ns}/{pvc_name_match.group(1)}"
        return pv

    def _parse_vsc_yaml(self, raw: str) -> Optional[K8sVSC]:
        name = self._yaml_val(raw, 'name', top_level=True)
        return K8sVSC(
            name=name or "",
            snapshot_handle=self._yaml_val(raw, 'snapshotHandle') or "",
            driver=self._yaml_val(raw, 'driver') or "",
            vs_name=self._yaml_val(raw, 'name', section='volumeSnapshotRef') or "",
            vs_namespace=self._yaml_val(raw, 'namespace', section='volumeSnapshotRef') or "",
            deletion_policy=self._yaml_val(raw, 'deletionPolicy') or "",
        )

    @staticmethod
    def _yaml_val(raw: str, key: str, top_level: bool = False, section: str = None) -> Optional[str]:
        """Quick regex extraction of a YAML value (not a full parser)."""
        if section:
            sec_match = re.search(rf'{section}:\s*\n((?:\s+.*\n)*)', raw)
            if sec_match:
                raw = sec_match.group(1)
        pattern = rf'^\s*{key}:\s*(.+)$' if not top_level else rf'^\s{{0,2}}{key}:\s*(.+)$'
        match = re.search(pattern, raw, re.MULTILINE)
        return match.group(1).strip().strip('"').strip("'") if match else None

    @staticmethod
    def _parse_rbd_info_text(raw: str) -> Optional[dict]:
        """Parse a single rbd info text block (indented key-value pairs).
        
        Example input:
            rbd image 'csi-vol-xxx':
                size 50 GiB in 12800 objects
                order 22 (4 MiB objects)
                snapshot_count: 0
                id: 1234abcd
                block_name_prefix: rbd_data.1234abcd
                format: 2
                features: layering, exclusive-lock, ...
                parent: pool/image@snap
                overlap: 50 GiB
        """
        info = {}
        for line in raw.splitlines():
            stripped = line.strip()
            if not stripped:
                continue

            # id field
            m = re.match(r'^id:\s*(\S+)', stripped)
            if m:
                info['id'] = m.group(1)
                continue

            # size field — "size 50 GiB in 12800 objects" or "size: 53687091200"
            m = re.match(r'^size[:\s]+(\d+(?:\.\d+)?)\s*(B|KiB|MiB|GiB|TiB|PiB|bytes?)(?:\s+in\s+\d+\s+objects)?', stripped)
            if m:
                val = float(m.group(1))
                unit = m.group(2).rstrip('s')
                multipliers = {'B': 1, 'byte': 1, 'KiB': 1024, 'MiB': 1024**2,
                               'GiB': 1024**3, 'TiB': 1024**4, 'PiB': 1024**5}
                info['size'] = int(val * multipliers.get(unit, 1))
                continue
            # size as raw bytes: "size: 53687091200"
            m = re.match(r'^size[:\s]+(\d{10,})', stripped)
            if m:
                info['size'] = int(m.group(1))
                continue

            # snapshot_count
            m = re.match(r'^snapshot_count:\s*(\d+)', stripped)
            if m:
                info['snapshot_count'] = int(m.group(1))
                continue

            # features
            m = re.match(r'^features:\s*(.+)', stripped)
            if m:
                info['features'] = [f.strip() for f in m.group(1).split(',') if f.strip()]
                continue

            # parent — "pool/image@snap" or "pool_ns/pool/image@snap"
            m = re.match(r'^parent:\s*(\S+)/(\S+)@(\S+)', stripped)
            if m:
                info['parent'] = {
                    'pool': m.group(1),
                    'image': m.group(2),
                    'snapshot': m.group(3),
                }
                continue

        return info if info else None

    def _parse_rbd_vol_and_snap_info(self, pool: str, raw: str) -> List[RBDImage]:
        """Parse the consolidated rbd_vol_and_snap_info_<pool> file.
        
        Format (repeating per image):
          Name of the block pool: <pool>
          Collecting image info for: <pool>/<image>
          rbd image '<image>':
                  size 50 GiB in 12800 objects
                  ...
                  id: 1234
                  ...
                  parent: pool/image@snap     (if clone)
                  ...
          Collecting image status for: <pool>/<image>
          Watchers:
                  watcher=...
          Collecting snap info for: <pool>/<image>
          [{"id":1,"name":"csi-snap-xxx","size":...,"protected":"true",...}]
          or
          []
        """
        images = []

        # Split into per-image sections using "Collecting image info for:" as delimiter
        sections = re.split(r'Collecting image info for:\s*', raw)

        for section in sections:
            if not section.strip():
                continue

            # First line should be "<pool>/<image_name>"
            lines = section.strip().splitlines()
            if not lines:
                continue

            header_match = re.match(r'(\S+)/(\S+)', lines[0].strip())
            if not header_match:
                continue

            img_pool = header_match.group(1)
            img_name = header_match.group(2)

            # Use the specified pool; skip if the section is for a different pool
            # (shouldn't happen in a per-pool file, but be safe)
            if img_pool != pool:
                continue

            # Extract the rbd info block: from "rbd image '...'" to the next major section
            info_text = ""
            snap_json_text = ""
            in_info = False
            in_snap = False

            for line in lines[1:]:
                stripped = line.strip()

                if stripped.startswith("rbd image '"):
                    in_info = True
                    in_snap = False
                    continue

                if stripped.startswith("Collecting image status for:"):
                    in_info = False
                    in_snap = False
                    continue

                if stripped.startswith("Collecting snap info for:"):
                    in_info = False
                    in_snap = True
                    continue

                if stripped.startswith("Collecting image info for:"):
                    # Next image section (shouldn't happen after split, but safety)
                    break

                if in_info:
                    info_text += line + "\n"
                elif in_snap:
                    snap_json_text += stripped

            # Parse image info
            info = self._parse_rbd_info_text(info_text)
            img = RBDImage(pool=pool, name=img_name)
            if info:
                img.image_id = info.get('id', '')
                img.size = info.get('size', 0)
                img.features = info.get('features', [])
                parent = info.get('parent', {})
                if parent:
                    img.parent_pool = parent.get('pool', '')
                    img.parent_image = parent.get('image', '')
                    img.parent_snap = parent.get('snapshot', '')

            # Parse snap info (JSON array)
            snap_json_text = snap_json_text.strip()
            if snap_json_text:
                try:
                    snaps = json.loads(snap_json_text)
                    if isinstance(snaps, list):
                        for s in snaps:
                            if isinstance(s, dict):
                                img.snapshots.append(RBDSnapshot(
                                    name=s.get('name', ''),
                                    snap_id=s.get('id', 0),
                                    size=s.get('size', 0),
                                    protected=str(s.get('protected', 'false')).lower() == 'true'
                                ))
                except json.JSONDecodeError:
                    pass

            images.append(img)

        return images

    @staticmethod
    def _parse_rbd_trash_text(pool: str, raw: str) -> List[RBDTrashEntry]:
        """Parse rbd trash ls plain text output.
        
        Typical formats:
          <trash_id> <name>
          <trash_id> <name> <status>
          <trash_id> <name> expires at <datetime>
        
        We extract the first two whitespace-separated tokens per line.
        """
        entries = []
        for line in raw.strip().splitlines():
            line = line.strip()
            if not line:
                continue
            # Skip header lines or noise
            if line.startswith('#') or line.startswith('NAME') or line.lower().startswith('id'):
                continue
            parts = line.split()
            if len(parts) >= 2:
                trash_id = parts[0]
                original_name = parts[1]
                entries.append(RBDTrashEntry(
                    pool=pool,
                    trash_id=trash_id,
                    original_name=original_name,
                ))
            elif len(parts) == 1:
                # Just an ID with no name
                entries.append(RBDTrashEntry(
                    pool=pool,
                    trash_id=parts[0],
                    original_name="",
                ))
        return entries


# ════════════════════════════════════════════════════════════════
# Analyzer — builds hierarchy and identifies orphans
# ════════════════════════════════════════════════════════════════

class Analyzer:
    """Builds hierarchical mappings and identifies orphan resources."""

    def __init__(self, collector: DataCollector):
        self.c = collector
        # Lookup maps (built during analysis)
        self.pv_by_image: Dict[str, K8sPV] = {}        # image_name -> PV
        self.pv_by_subvol: Dict[str, K8sPV] = {}       # subvol_name -> PV
        self.vsc_by_handle_uuid: Dict[str, K8sVSC] = {}# uuid part of handle -> VSC
        self.vsc_by_name: Dict[str, K8sVSC] = {}       # vsc name -> VSC
        self.vs_by_name_ns: Dict[str, K8sVolumeSnapshot] = {}  # "ns/name" -> VS
        self.image_by_name: Dict[str, RBDImage] = {}    # "pool/name" -> RBDImage
        self.trash_by_name: Dict[str, RBDTrashEntry] = {}  # "pool/original_name" -> trash
        self.subvol_by_name: Dict[str, CephFSSubvolume] = {}  # name -> subvol

        # Results
        self.orphan_rbd_images: List[RBDImage] = []
        self.orphan_cephfs_subvols: List[CephFSSubvolume] = []
        self.orphan_rbd_snaps: List[Tuple[RBDImage, RBDSnapshot]] = []  # (image, snap)
        self.orphan_cephfs_snaps: List[Tuple[CephFSSubvolume, str]] = []  # (subvol, snap_name)
        self.orphan_vscs: List[K8sVSC] = []
        self.orphan_vs: List[K8sVolumeSnapshot] = []
        self.dangling_pvs_rbd: List[K8sPV] = []       # PV with no matching Ceph image
        self.dangling_pvs_cephfs: List[K8sPV] = []    # PV with no matching subvolume
        self.trash_with_children: List[Tuple[RBDTrashEntry, List[str]]] = []

    def analyze(self):
        """Run full analysis."""
        print(f"\n{C.BOLD}{'='*60}{C.RESET}")
        print(f"{C.BOLD}  ANALYSIS{C.RESET}")
        print(f"{C.BOLD}{'='*60}{C.RESET}\n")

        self._build_lookups()
        self._find_rbd_orphans()
        self._find_cephfs_orphans()
        self._find_snapshot_orphans()
        self._find_vsc_orphans()
        self._find_dangling_pvs()
        self._find_trash_with_live_children()

    def _build_lookups(self):
        print(f"{C.CYAN}[ANALYZE]{C.RESET} Building lookup maps...")

        for pv in self.c.pvs:
            if pv.image_name:
                self.pv_by_image[pv.image_name] = pv
            if pv.subvolume_name:
                self.pv_by_subvol[pv.subvolume_name] = pv

        for vsc in self.c.vscs:
            self.vsc_by_name[vsc.name] = vsc
            # Extract UUID from snapshot handle
            # Handle format: <pool/cephfs-prefix>-<uuid> or just the UUID tail
            handle = vsc.snapshot_handle
            if handle:
                # Try extracting last 5 dash-separated groups as UUID
                parts = handle.split('-')
                if len(parts) >= 5:
                    uuid = '-'.join(parts[-5:])
                    self.vsc_by_handle_uuid[uuid] = vsc
                # Also store full handle for exact matching
                self.vsc_by_handle_uuid[handle] = vsc

        for vs in self.c.volume_snapshots:
            key = f"{vs.namespace}/{vs.name}"
            self.vs_by_name_ns[key] = vs

        for pool, images in self.c.rbd_images.items():
            for img in images:
                self.image_by_name[f"{pool}/{img.name}"] = img

        for pool, trash in self.c.rbd_trash.items():
            for entry in trash:
                self.trash_by_name[f"{pool}/{entry.original_name}"] = entry

        for sv in self.c.cephfs_subvolumes:
            self.subvol_by_name[sv.name] = sv

    def _find_rbd_orphans(self):
        """RBD images (csi-vol-* and csi-snap-*) without matching K8s objects."""
        print(f"{C.CYAN}[ANALYZE]{C.RESET} Checking RBD images for orphans...")

        for pool, images in self.c.rbd_images.items():
            for img in images:
                if img.name.startswith('csi-vol-'):
                    # csi-vol-* should have a matching PV
                    if img.name not in self.pv_by_image:
                        self.orphan_rbd_images.append(img)

                elif img.name.startswith('csi-snap-'):
                    # csi-snap-* is a clone child image backing a VSC
                    # Extract the UUID (strip "csi-snap-" prefix)
                    snap_uuid = img.name[len('csi-snap-'):]
                    if snap_uuid not in self.vsc_by_handle_uuid:
                        # Check if ANY VSC handle contains this UUID
                        found = False
                        for handle in self.vsc_by_handle_uuid:
                            if snap_uuid in handle:
                                found = True
                                break
                        if not found:
                            self.orphan_rbd_images.append(img)

    def _find_cephfs_orphans(self):
        """CephFS subvolumes without matching PV."""
        print(f"{C.CYAN}[ANALYZE]{C.RESET} Checking CephFS subvolumes for orphans...")

        for sv in self.c.cephfs_subvolumes:
            if sv.name not in self.pv_by_subvol:
                self.orphan_cephfs_subvols.append(sv)

    def _find_snapshot_orphans(self):
        """RBD and CephFS snapshots (csi-snap-*) without matching VSC."""
        print(f"{C.CYAN}[ANALYZE]{C.RESET} Checking snapshots for orphans...")

        for pool, images in self.c.rbd_images.items():
            for img in images:
                for snap in img.snapshots:
                    if snap.name.startswith('csi-snap-'):
                        snap_uuid = snap.name[len('csi-snap-'):]
                        found = any(snap_uuid in h for h in self.vsc_by_handle_uuid)
                        if not found:
                            self.orphan_rbd_snaps.append((img, snap))

        for sv in self.c.cephfs_subvolumes:
            for snap_name in sv.snapshots:
                if snap_name.startswith('csi-snap-'):
                    snap_uuid = snap_name[len('csi-snap-'):]
                    found = any(snap_uuid in h for h in self.vsc_by_handle_uuid)
                    if not found:
                        self.orphan_cephfs_snaps.append((sv, snap_name))

    def _find_vsc_orphans(self):
        """VSCs whose referenced VolumeSnapshot doesn't exist."""
        print(f"{C.CYAN}[ANALYZE]{C.RESET} Checking VolumeSnapshotContents for orphans...")

        for vsc in self.c.vscs:
            if vsc.vs_name and vsc.vs_namespace:
                key = f"{vsc.vs_namespace}/{vsc.vs_name}"
                if key not in self.vs_by_name_ns:
                    self.orphan_vscs.append(vsc)

    def _find_dangling_pvs(self):
        """PVs whose backing Ceph resource doesn't exist (reverse orphan)."""
        print(f"{C.CYAN}[ANALYZE]{C.RESET} Checking for dangling PVs (no backing Ceph resource)...")

        all_rbd_names = set()
        for pool, images in self.c.rbd_images.items():
            for img in images:
                all_rbd_names.add(img.name)
        # Also include trash entries (they still exist, just in trash)
        all_trash_names = set()
        for pool, trash in self.c.rbd_trash.items():
            for t in trash:
                all_trash_names.add(t.original_name)

        all_subvol_names = set(sv.name for sv in self.c.cephfs_subvolumes)

        for pv in self.c.pvs:
            if pv.image_name:
                if pv.image_name not in all_rbd_names and pv.image_name not in all_trash_names:
                    self.dangling_pvs_rbd.append(pv)
            if pv.subvolume_name:
                if pv.subvolume_name not in all_subvol_names:
                    self.dangling_pvs_cephfs.append(pv)

    def _find_trash_with_live_children(self):
        """Identify trashed RBD images that have live children in the pool."""
        print(f"{C.CYAN}[ANALYZE]{C.RESET} Checking trash entries for live children...")

        for pool, trash in self.c.rbd_trash.items():
            for entry in trash:
                children = []
                # Check if any pool image has this trash entry as parent
                for p, images in self.c.rbd_images.items():
                    for img in images:
                        if (img.parent_image == entry.original_name and
                            img.parent_pool == pool):
                            children.append(f"{p}/{img.name}")
                if children:
                    self.trash_with_children.append((entry, children))

    # ──────────────────────────────────────────
    # Hierarchy helpers for report
    # ──────────────────────────────────────────

    def get_vsc_for_snap_uuid(self, snap_uuid: str) -> Optional[K8sVSC]:
        """Find the VSC matching a csi-snap UUID."""
        for handle, vsc in self.vsc_by_handle_uuid.items():
            if snap_uuid in handle:
                return vsc
        return None

    def get_vs_for_vsc(self, vsc: K8sVSC) -> Optional[K8sVolumeSnapshot]:
        """Find the VolumeSnapshot referenced by a VSC."""
        key = f"{vsc.vs_namespace}/{vsc.vs_name}"
        return self.vs_by_name_ns.get(key)

    def get_children_of_image(self, pool: str, image_name: str) -> List[RBDImage]:
        """Find all RBD images that are clones of pool/image_name@<some_snap>."""
        children = []
        for p, images in self.c.rbd_images.items():
            for img in images:
                if img.parent_image == image_name and img.parent_pool == pool:
                    children.append(img)
        return children

    def get_pv_for_image(self, image_name: str) -> Optional[K8sPV]:
        return self.pv_by_image.get(image_name)

    def get_pv_for_subvol(self, subvol_name: str) -> Optional[K8sPV]:
        return self.pv_by_subvol.get(subvol_name)


# ════════════════════════════════════════════════════════════════
# Report Generator
# ════════════════════════════════════════════════════════════════

class ReportGenerator:
    """Generates human-readable and machine-readable reports."""

    def __init__(self, collector: DataCollector, analyzer: Analyzer):
        self.c = collector
        self.a = analyzer
        self.lines = []

    def generate(self, output_file: str = None, json_output: str = None):
        """Generate all report sections."""
        self._header()
        self._rbd_hierarchy()
        self._cephfs_hierarchy()
        self._rbd_trash_section()
        self._orphan_summary()
        self._counts_summary()

        report_text = "\n".join(self.lines)

        # Print to stdout
        print(report_text)

        # Write to file
        if output_file:
            # Strip ANSI codes for file output
            clean = re.sub(r'\033\[[0-9;]*m', '', report_text)
            with open(output_file, 'w') as f:
                f.write(clean)
            print(f"\n{C.GREEN}Report saved to: {output_file}{C.RESET}")

        # Write JSON
        if json_output:
            self._write_json(json_output)
            print(f"{C.GREEN}JSON report saved to: {json_output}{C.RESET}")

    def _p(self, line: str = ""):
        self.lines.append(line)

    def _header(self):
        self._p(f"\n{'═'*70}")
        self._p(f"  ODF ORPHAN RESOURCE FINDER — REPORT")
        self._p(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self._p(f"  Mode: {'Live Cluster' if isinstance(self.c.runner, LiveRunner) else 'Must-Gather'}")
        self._p(f"{'═'*70}")

    # ──────────────────────────────────────────
    # RBD Hierarchy
    # ──────────────────────────────────────────

    def _rbd_hierarchy(self):
        self._p(f"\n{'═'*70}")
        self._p(f"  {C.BOLD}HIERARCHICAL MAPPING — RBD{C.RESET}")
        self._p(f"{'═'*70}")

        for pool in self.c.rbd_pools:
            images = self.c.rbd_images.get(pool, [])
            if not images:
                continue

            self._p(f"\n{C.CYAN}Pool: {pool}{C.RESET}")
            self._p(f"{'─'*50}")

            # Separate root images (csi-vol-*) from child images (csi-snap-*)
            root_images = [i for i in images if i.name.startswith('csi-vol-')]
            child_images = [i for i in images if i.name.startswith('csi-snap-')]
            other_images = [i for i in images
                           if not i.name.startswith('csi-vol-') and not i.name.startswith('csi-snap-')]

            # Process root images (csi-vol-*)
            for img in root_images:
                pv = self.a.get_pv_for_image(img.name)
                self._render_rbd_tree(pool, img, pv, indent=1)

            # Process child images that have no visible parent in pool (orphan clones)
            for img in child_images:
                # Skip if we already rendered it as part of a parent tree
                # (we handle this inside _render_rbd_tree)
                parent_key = f"{img.parent_pool}/{img.parent_image}" if img.parent_image else ""
                parent_in_pool = parent_key in self.a.image_by_name
                parent_in_trash = parent_key in self.a.trash_by_name
                if not parent_in_pool and not parent_in_trash:
                    # Standalone child with no visible parent
                    self._p(f"\n  {C.YELLOW}[CHILD IMAGE — parent not found]{C.RESET}")
                    self._render_rbd_image_line(pool, img, indent=2)

            for img in other_images:
                self._p(f"\n  Image: {img.name} (id: {img.image_id})")

    def _render_rbd_tree(self, pool: str, img: RBDImage, pv: Optional[K8sPV],
                         indent: int = 1, rendered: set = None):
        """Recursively render an RBD image tree with its snapshots and clone children."""
        if rendered is None:
            rendered = set()
        if img.name in rendered:
            return
        rendered.add(img.name)

        pad = "  " * indent
        size_str = self._human_size(img.size) if img.size else "?"

        if pv:
            status_color = C.GREEN if pv.status == 'Bound' else C.YELLOW
            self._p(f"\n{pad}{C.GREEN}PV: {pv.name}{C.RESET} ({status_color}{pv.status}{C.RESET}, {pv.capacity})"
                    f"  {C.DIM}claim: {pv.claim_ref}{C.RESET}")
            self._p(f"{pad}└── Image: {pool}/{img.name} (id: {img.image_id}, {size_str})")
        else:
            orphan_tag = f" {C.RED}◀ ORPHAN — no matching PV{C.RESET}" if img.name.startswith('csi-vol-') else ""
            self._p(f"\n{pad}{C.RED if img.name.startswith('csi-vol-') else C.YELLOW}"
                    f"Image: {pool}/{img.name}{C.RESET} (id: {img.image_id}, {size_str}){orphan_tag}")

        # Parent info (if this image is a clone)
        if img.parent_image:
            self._p(f"{pad}    {C.DIM}parent: {img.parent_pool}/{img.parent_image}@{img.parent_snap}"
                    f" (COW — not flattened){C.RESET}")

        # Snapshots on this image
        snap_pad = f"{pad}    "
        csi_snaps = [s for s in img.snapshots if s.name.startswith('csi-snap-')]
        other_snaps = [s for s in img.snapshots if not s.name.startswith('csi-snap-')]

        for i, snap in enumerate(csi_snaps):
            is_last_snap = (i == len(csi_snaps) - 1) and not other_snaps
            connector = "└──" if is_last_snap else "├──"
            snap_uuid = snap.name[len('csi-snap-'):]
            vsc = self.a.get_vsc_for_snap_uuid(snap_uuid)

            if vsc:
                vs = self.a.get_vs_for_vsc(vsc)
                vs_str = f" → VS: {vs.namespace}/{vs.name}" if vs else f" → VS: {C.RED}MISSING{C.RESET}"
                self._p(f"{snap_pad}{connector} Snap: {snap.name}"
                        f"  {C.DIM}VSC: {vsc.name}{vs_str}{C.RESET}")
            else:
                self._p(f"{snap_pad}{connector} Snap: {snap.name}"
                        f"  {C.RED}◀ ORPHAN — no matching VSC{C.RESET}")

            # Find clone child image created from this snapshot
            children = self.a.get_children_of_image(pool, img.name)
            clone_from_this_snap = [c for c in children if c.parent_snap == snap.name]
            sub_connector = "    " if is_last_snap else "│   "
            for child in clone_from_this_snap:
                child_pv = self.a.get_pv_for_image(child.name)
                self._p(f"{snap_pad}{sub_connector}└── Clone: {child.name}"
                        f" (id: {child.image_id}, COW child)")
                if child_pv:
                    self._p(f"{snap_pad}{sub_connector}    └── Restored PV: {child_pv.name}"
                            f" ({child_pv.status}, {child_pv.capacity})")
                # Recurse into child's own snapshots
                if child.snapshots:
                    for cs in child.snapshots:
                        if cs.name.startswith('csi-snap-'):
                            cs_uuid = cs.name[len('csi-snap-'):]
                            cs_vsc = self.a.get_vsc_for_snap_uuid(cs_uuid)
                            tag = f"VSC: {cs_vsc.name}" if cs_vsc else f"{C.RED}ORPHAN snap{C.RESET}"
                            self._p(f"{snap_pad}{sub_connector}        └── Snap: {cs.name} ({tag})")

        for snap in other_snaps:
            self._p(f"{snap_pad}├── Snap: {snap.name} {C.DIM}(non-CSI){C.RESET}")

    def _render_rbd_image_line(self, pool: str, img: RBDImage, indent: int):
        pad = "  " * indent
        size_str = self._human_size(img.size) if img.size else "?"
        self._p(f"{pad}Image: {pool}/{img.name} (id: {img.image_id}, {size_str})")
        if img.parent_image:
            self._p(f"{pad}  parent: {img.parent_pool}/{img.parent_image}@{img.parent_snap}")

    # ──────────────────────────────────────────
    # CephFS Hierarchy
    # ──────────────────────────────────────────

    def _cephfs_hierarchy(self):
        if not self.c.cephfs_subvolumes:
            return

        self._p(f"\n{'═'*70}")
        self._p(f"  {C.BOLD}HIERARCHICAL MAPPING — CephFS{C.RESET}")
        self._p(f"{'═'*70}")
        self._p(f"\n{C.CYAN}Filesystem: {self.c.cephfs_name}{C.RESET}")
        self._p(f"{'─'*50}")

        for sv in self.c.cephfs_subvolumes:
            pv = self.a.get_pv_for_subvol(sv.name)
            type_tag = f" (type: {sv.vol_type})" if sv.vol_type else ""

            if pv:
                status_color = C.GREEN if pv.status == 'Bound' else C.YELLOW
                self._p(f"\n  {C.GREEN}PV: {pv.name}{C.RESET} ({status_color}{pv.status}{C.RESET},"
                        f" {pv.capacity})  {C.DIM}claim: {pv.claim_ref}{C.RESET}")
                self._p(f"  └── Subvolume: csi/{sv.name}{type_tag}")
            else:
                self._p(f"\n  {C.RED}Subvolume: csi/{sv.name}{type_tag}"
                        f"  ◀ ORPHAN — no matching PV{C.RESET}")

            # Snapshots
            for i, snap_name in enumerate(sv.snapshots):
                is_last = (i == len(sv.snapshots) - 1)
                connector = "└──" if is_last else "├──"
                if snap_name.startswith('csi-snap-'):
                    snap_uuid = snap_name[len('csi-snap-'):]
                    vsc = self.a.get_vsc_for_snap_uuid(snap_uuid)
                    if vsc:
                        vs = self.a.get_vs_for_vsc(vsc)
                        vs_str = f" → VS: {vs.namespace}/{vs.name}" if vs else ""
                        self._p(f"      {connector} Snap: {snap_name}"
                                f"  {C.DIM}VSC: {vsc.name}{vs_str}{C.RESET}")
                    else:
                        self._p(f"      {connector} Snap: {snap_name}"
                                f"  {C.RED}◀ ORPHAN — no matching VSC{C.RESET}")
                else:
                    self._p(f"      {connector} Snap: {snap_name} {C.DIM}(non-CSI){C.RESET}")

    # ──────────────────────────────────────────
    # RBD Trash
    # ──────────────────────────────────────────

    def _rbd_trash_section(self):
        has_trash = any(len(entries) > 0 for entries in self.c.rbd_trash.values())
        if not has_trash:
            return

        self._p(f"\n{'═'*70}")
        self._p(f"  {C.BOLD}RBD TRASH{C.RESET}")
        self._p(f"{'═'*70}")

        for pool, entries in self.c.rbd_trash.items():
            if not entries:
                continue
            self._p(f"\n{C.CYAN}Pool: {pool}{C.RESET}")
            self._p(f"{'─'*50}")

            for entry in entries:
                size_str = self._human_size(entry.size) if entry.size else "?"
                self._p(f"\n  Trash: {entry.original_name} (trash_id: {entry.trash_id}, {size_str})")
                if entry.parent_image:
                    self._p(f"    parent: {entry.parent_pool}/{entry.parent_image}@{entry.parent_snap}")

                # Show snapshots still on trashed image
                for snap in entry.snapshots:
                    protected_tag = f" {C.YELLOW}[PROTECTED]{C.RESET}" if snap.protected else ""
                    self._p(f"    ├── Snap: {snap.name}{protected_tag}")

                # Show live children in pool
                for te, children in self.a.trash_with_children:
                    if te.trash_id == entry.trash_id and te.pool == pool:
                        for ch in children:
                            self._p(f"    └── {C.MAGENTA}Live child in pool: {ch}{C.RESET}")
                        if children:
                            self._p(f"        {C.YELLOW}⚠  Cannot purge from trash until children are removed/flattened{C.RESET}")

    # ──────────────────────────────────────────
    # Orphan Summary
    # ──────────────────────────────────────────

    def _orphan_summary(self):
        self._p(f"\n{'═'*70}")
        self._p(f"  {C.BOLD}ORPHAN DETAILS{C.RESET}")
        self._p(f"{'═'*70}")

        # Orphan RBD Images
        self._p(f"\n{C.BOLD}Orphan RBD Images (no matching PV or VSC):{C.RESET}")
        if self.a.orphan_rbd_images:
            for img in self.a.orphan_rbd_images:
                size_str = self._human_size(img.size) if img.size else "?"
                parent_str = ""
                if img.parent_image:
                    parent_str = f" (cloned from: {img.parent_pool}/{img.parent_image}@{img.parent_snap})"
                self._p(f"  {C.RED}✗{C.RESET} {img.pool}/{img.name} — {size_str}{parent_str}")
        else:
            self._p(f"  {C.GREEN}✓ None found{C.RESET}")

        # Orphan CephFS Subvolumes
        self._p(f"\n{C.BOLD}Orphan CephFS Subvolumes (no matching PV):{C.RESET}")
        if self.a.orphan_cephfs_subvols:
            for sv in self.a.orphan_cephfs_subvols:
                type_tag = f" (type: {sv.vol_type})" if sv.vol_type else ""
                self._p(f"  {C.RED}✗{C.RESET} csi/{sv.name}{type_tag}")
        else:
            self._p(f"  {C.GREEN}✓ None found{C.RESET}")

        # Orphan RBD Snapshots
        self._p(f"\n{C.BOLD}Orphan RBD Snapshots (no matching VSC):{C.RESET}")
        if self.a.orphan_rbd_snaps:
            for img, snap in self.a.orphan_rbd_snaps:
                self._p(f"  {C.RED}✗{C.RESET} {img.pool}/{img.name}@{snap.name}")
        else:
            self._p(f"  {C.GREEN}✓ None found{C.RESET}")

        # Orphan CephFS Snapshots
        self._p(f"\n{C.BOLD}Orphan CephFS Snapshots (no matching VSC):{C.RESET}")
        if self.a.orphan_cephfs_snaps:
            for sv, snap_name in self.a.orphan_cephfs_snaps:
                self._p(f"  {C.RED}✗{C.RESET} csi/{sv.name}@{snap_name}")
        else:
            self._p(f"  {C.GREEN}✓ None found{C.RESET}")

        # Orphan VSCs
        self._p(f"\n{C.BOLD}Orphan VolumeSnapshotContents (VS missing):{C.RESET}")
        if self.a.orphan_vscs:
            for vsc in self.a.orphan_vscs:
                self._p(f"  {C.RED}✗{C.RESET} {vsc.name}  (expected VS: {vsc.vs_namespace}/{vsc.vs_name})")
        else:
            self._p(f"  {C.GREEN}✓ None found{C.RESET}")

        # Dangling PVs
        if self.a.dangling_pvs_rbd or self.a.dangling_pvs_cephfs:
            self._p(f"\n{C.BOLD}Dangling PVs (PV exists but backing Ceph resource missing):{C.RESET}")
            for pv in self.a.dangling_pvs_rbd:
                self._p(f"  {C.YELLOW}⚠{C.RESET} PV: {pv.name} → RBD image '{pv.image_name}' not found in pool '{pv.pool}'")
            for pv in self.a.dangling_pvs_cephfs:
                self._p(f"  {C.YELLOW}⚠{C.RESET} PV: {pv.name} → CephFS subvol '{pv.subvolume_name}' not found")

        # Trash entries with live children
        if self.a.trash_with_children:
            self._p(f"\n{C.BOLD}Trash Images with Live Children (blocked purge):{C.RESET}")
            for entry, children in self.a.trash_with_children:
                self._p(f"  {C.MAGENTA}⚠{C.RESET} {entry.pool}/{entry.original_name} (trash_id: {entry.trash_id})")
                for ch in children:
                    self._p(f"      └── child: {ch}")
                self._p(f"      {C.DIM}Cleanup: recover parent → flatten child → delete child → delete parent{C.RESET}")

    # ──────────────────────────────────────────
    # Counts Summary
    # ──────────────────────────────────────────

    def _counts_summary(self):
        self._p(f"\n{'═'*70}")
        self._p(f"  {C.BOLD}SUMMARY COUNTS{C.RESET}")
        self._p(f"{'═'*70}")

        total_rbd = sum(len(imgs) for imgs in self.c.rbd_images.values())
        total_trash = sum(len(t) for t in self.c.rbd_trash.values())
        total_rbd_snaps = sum(
            len(img.snapshots) for imgs in self.c.rbd_images.values() for img in imgs
        )
        total_cephfs = len(self.c.cephfs_subvolumes)
        total_cephfs_snaps = sum(len(sv.snapshots) for sv in self.c.cephfs_subvolumes)

        rbd_pvs = len([p for p in self.c.pvs if p.image_name])
        cephfs_pvs = len([p for p in self.c.pvs if p.subvolume_name])

        w = 50
        self._p(f"\n{'─'*70}")
        self._p(f"  {'Resource':<{w}} {'Count':>8}")
        self._p(f"{'─'*70}")
        self._p(f"  {'RBD PVs (OCP)':<{w}} {rbd_pvs:>8}")
        self._p(f"  {'RBD Images (Ceph pool)':<{w}} {total_rbd:>8}")
        self._p(f"  {'RBD Snapshots (on images)':<{w}} {total_rbd_snaps:>8}")
        self._p(f"  {'RBD Trash entries':<{w}} {total_trash:>8}")
        self._p(f"  {C.RED + 'Orphan RBD Images' + C.RESET:<{w+len(C.RED)+len(C.RESET)}} {len(self.a.orphan_rbd_images):>8}")
        self._p(f"  {C.RED + 'Orphan RBD Snapshots' + C.RESET:<{w+len(C.RED)+len(C.RESET)}} {len(self.a.orphan_rbd_snaps):>8}")
        self._p(f"{'─'*70}")
        self._p(f"  {'CephFS PVs (OCP)':<{w}} {cephfs_pvs:>8}")
        self._p(f"  {'CephFS Subvolumes (Ceph)':<{w}} {total_cephfs:>8}")
        self._p(f"  {'CephFS Snapshots (on subvolumes)':<{w}} {total_cephfs_snaps:>8}")
        self._p(f"  {C.RED + 'Orphan CephFS Subvolumes' + C.RESET:<{w+len(C.RED)+len(C.RESET)}} {len(self.a.orphan_cephfs_subvols):>8}")
        self._p(f"  {C.RED + 'Orphan CephFS Snapshots' + C.RESET:<{w+len(C.RED)+len(C.RESET)}} {len(self.a.orphan_cephfs_snaps):>8}")
        self._p(f"{'─'*70}")
        self._p(f"  {'VolumeSnapshotContents (OCP)':<{w}} {len(self.c.vscs):>8}")
        self._p(f"  {'VolumeSnapshots (OCP)':<{w}} {len(self.c.volume_snapshots):>8}")
        self._p(f"  {C.RED + 'Orphan VSCs' + C.RESET:<{w+len(C.RED)+len(C.RESET)}} {len(self.a.orphan_vscs):>8}")
        self._p(f"{'─'*70}")
        if self.a.dangling_pvs_rbd or self.a.dangling_pvs_cephfs:
            self._p(f"  {C.YELLOW + 'Dangling PVs (no Ceph resource)' + C.RESET:<{w+len(C.YELLOW)+len(C.RESET)}} "
                    f"{len(self.a.dangling_pvs_rbd) + len(self.a.dangling_pvs_cephfs):>8}")
        if self.a.trash_with_children:
            self._p(f"  {C.MAGENTA + 'Trash with live children (blocked)' + C.RESET:<{w+len(C.MAGENTA)+len(C.RESET)}} "
                    f"{len(self.a.trash_with_children):>8}")
        self._p(f"{'═'*70}\n")

    # ──────────────────────────────────────────
    # JSON output
    # ──────────────────────────────────────────

    def _write_json(self, path: str):
        data = {
            "generated": datetime.now().isoformat(),
            "summary": {
                "rbd_pvs": len([p for p in self.c.pvs if p.image_name]),
                "rbd_images": sum(len(imgs) for imgs in self.c.rbd_images.values()),
                "rbd_trash": sum(len(t) for t in self.c.rbd_trash.values()),
                "cephfs_pvs": len([p for p in self.c.pvs if p.subvolume_name]),
                "cephfs_subvolumes": len(self.c.cephfs_subvolumes),
                "orphan_rbd_images": len(self.a.orphan_rbd_images),
                "orphan_cephfs_subvolumes": len(self.a.orphan_cephfs_subvols),
                "orphan_rbd_snapshots": len(self.a.orphan_rbd_snaps),
                "orphan_cephfs_snapshots": len(self.a.orphan_cephfs_snaps),
                "orphan_vscs": len(self.a.orphan_vscs),
                "dangling_pvs": len(self.a.dangling_pvs_rbd) + len(self.a.dangling_pvs_cephfs),
                "trash_blocked": len(self.a.trash_with_children),
            },
            "orphans": {
                "rbd_images": [
                    {"pool": i.pool, "name": i.name, "id": i.image_id, "size": i.size,
                     "parent": f"{i.parent_pool}/{i.parent_image}@{i.parent_snap}" if i.parent_image else ""}
                    for i in self.a.orphan_rbd_images
                ],
                "cephfs_subvolumes": [
                    {"name": sv.name, "type": sv.vol_type, "path": sv.path}
                    for sv in self.a.orphan_cephfs_subvols
                ],
                "rbd_snapshots": [
                    {"image": f"{img.pool}/{img.name}", "snapshot": snap.name}
                    for img, snap in self.a.orphan_rbd_snaps
                ],
                "cephfs_snapshots": [
                    {"subvolume": sv.name, "snapshot": snap}
                    for sv, snap in self.a.orphan_cephfs_snaps
                ],
                "vscs": [
                    {"name": vsc.name, "expected_vs": f"{vsc.vs_namespace}/{vsc.vs_name}",
                     "handle": vsc.snapshot_handle}
                    for vsc in self.a.orphan_vscs
                ],
                "dangling_pvs": [
                    {"pv": pv.name, "missing_image": pv.image_name, "pool": pv.pool}
                    for pv in self.a.dangling_pvs_rbd
                ] + [
                    {"pv": pv.name, "missing_subvolume": pv.subvolume_name}
                    for pv in self.a.dangling_pvs_cephfs
                ],
            },
            "trash_blocked": [
                {"pool": entry.pool, "name": entry.original_name, "trash_id": entry.trash_id,
                 "live_children": children}
                for entry, children in self.a.trash_with_children
            ],
        }
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)

    # ──────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────

    @staticmethod
    def _human_size(size_bytes: int) -> str:
        if size_bytes == 0:
            return "0B"
        for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB']:
            if abs(size_bytes) < 1024.0:
                return f"{size_bytes:.1f}{unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f}PiB"


# ════════════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="ODF Orphan Resource Finder — identifies orphan RBD images, CephFS subvolumes, "
                    "snapshots, and their hierarchical relationships.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Live cluster (oc must be logged in)
  python3 odf_orphan_finder.py --live

  # Live cluster with custom namespace and output
  python3 odf_orphan_finder.py --live --namespace openshift-storage --output report.txt --json report.json

  # Must-gather (offline)
  python3 odf_orphan_finder.py --must-gather /path/to/must-gather

  # No colors (for piping)
  python3 odf_orphan_finder.py --live --no-color | tee report.txt
        """
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--live', action='store_true',
                      help='Run against a live OCP cluster (requires oc login)')
    mode.add_argument('--must-gather', metavar='PATH',
                      help='Path to an ODF must-gather directory')

    parser.add_argument('--namespace', '-n', default='openshift-storage',
                        help='Rook-Ceph namespace (default: openshift-storage)')
    parser.add_argument('--output', '-o', metavar='FILE',
                        help='Save text report to file')
    parser.add_argument('--json', metavar='FILE', dest='json_output',
                        help='Save JSON report to file')
    parser.add_argument('--no-color', action='store_true',
                        help='Disable colored output')

    args = parser.parse_args()

    if args.no_color:
        C.disable()

    print(f"\n{C.BOLD}╔══════════════════════════════════════════════════════════════╗{C.RESET}")
    print(f"{C.BOLD}║          ODF ORPHAN RESOURCE FINDER                         ║{C.RESET}")
    print(f"{C.BOLD}║          Read-only analysis — no changes made               ║{C.RESET}")
    print(f"{C.BOLD}╚══════════════════════════════════════════════════════════════╝{C.RESET}")

    # Initialize runner
    if args.live:
        runner = LiveRunner(namespace=args.namespace)
    else:
        runner = MustGatherRunner(args.must_gather)

    # Collect data
    collector = DataCollector(runner)
    collector.collect_all()

    # Analyze
    analyzer = Analyzer(collector)
    analyzer.analyze()

    # Report
    reporter = ReportGenerator(collector, analyzer)
    reporter.generate(
        output_file=args.output,
        json_output=args.json_output,
    )


if __name__ == '__main__':
    main()