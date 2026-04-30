#!/usr/bin/env python3
"""
Render a Ceph RBD parent / snapshot / child tree from the JSON produced
by the tree builder script.

Usage:
    python3 rbd_tree.py tree.json
    cat tree.json | python3 rbd_tree.py
"""
import json
import sys

# Tree-drawing glyphs
T_BRANCH = "├── "
L_BRANCH = "└── "
V_PIPE   = "│   "
SPACE    = "    "


def _fmt_image_header(img: dict) -> list[str]:
    """Return the lines that describe an image node (header + extras)."""
    flags = []
    if img.get("trash"):
        flags.append("TRASH")
    if img.get("namespace"):
        flags.append(f"ns={img['namespace']}")
    flag_str = f"  [{', '.join(flags)}]" if flags else ""

    icon = "🗑️ " if img.get("trash") else "📦 "
    lines = [
        f"{icon}{img['imageName']}{flag_str}  "
        f"(id={img['imageId']}, pool={img['pool']})"
    ]

    pv = img.get("pv")
    if pv:
        lines.append(f"   ↳ PV: {pv['pvName']}  (owner: {pv['volumeOwner']})")

    sc = img.get("snapshotContent")
    if sc:
        lines.append(
            f"   ↳ SnapContent: {sc['snapContentName']}  "
            f"← source: {sc['source']}  (owner: {sc['volumeOwner']})"
        )
    return lines


def _print_image(img: dict, prefix: str, is_last: bool, out: list[str]) -> None:
    connector   = L_BRANCH if is_last else T_BRANCH
    next_prefix = prefix + (SPACE if is_last else V_PIPE)

    head, *rest = _fmt_image_header(img)
    out.append(prefix + connector + head)
    for r in rest:
        out.append(next_prefix + r)

    snaps = img.get("snapshots") or []
    for i, snap in enumerate(snaps):
        _print_snap(snap, next_prefix, i == len(snaps) - 1, out)


def _print_snap(snap: dict, prefix: str, is_last: bool, out: list[str]) -> None:
    connector   = L_BRANCH if is_last else T_BRANCH
    next_prefix = prefix + (SPACE if is_last else V_PIPE)

    out.append(
        prefix + connector
        + f"📸 snap@{snap['snapId']}: {snap['snapName']}"
    )

    children = snap.get("children") or []
    for i, child in enumerate(children):
        _print_image(child, next_prefix, i == len(children) - 1, out)


def render(data: dict) -> str:
    out: list[str] = []
    volumes = data.get("volumes", []) or []
    out.append(f"RBD tree — {len(volumes)} top-level image(s)")
    out.append("=" * 60)

    for i, v in enumerate(volumes):
        _print_image(v, "", i == len(volumes) - 1, out)
        if i != len(volumes) - 1:
            out.append("")  # blank line between top-level images

    orphans = data.get("orphaned_pv") or []
    if orphans:
        out.append("")
        out.append(f"Orphaned PVs ({len(orphans)}):")
        for o in orphans:
            out.append(f"  - {o}")
    return "\n".join(out)


if __name__ == "__main__":
    src = open(sys.argv[1]) if len(sys.argv) > 1 else sys.stdin
    print(render(json.load(src)))