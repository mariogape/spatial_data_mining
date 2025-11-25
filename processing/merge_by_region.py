#!/usr/bin/env python3
"""
Plug-and-play script to merge GeoTIFF tiles that share the same region token in
their filenames. Edit the USER SETTINGS section below, then run:

    python processing/merge_by_region.py

Default pattern expects files like:
    meta_canopy_height_<region>-0000000000-0000065536.tif

Requires GDAL's gdalwarp on PATH (from OSGeo4W, QGIS, etc.).
"""

import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional

###############################################################################
# USER SETTINGS - EDIT THESE
###############################################################################

# Folder containing the source .tif/.tiff tiles (searched recursively)
INPUT_FOLDER = r"D:\OpenPas Spatial Data\Forest Canopy Height\GEE_canopy_height-20251125T162002Z-1-001\GEE_canopy_height"

# Folder where merged rasters will be written
OUTPUT_FOLDER = r"D:\OpenPas Spatial Data\Forest Canopy Height\GEE_canopy_height\merged"

# Text that appears before the region token in the filename stem
PREFIX = "meta_canopy_height_"

# Characters that separate the region token from the per-tile id
SEPARATORS = "-_"

# Suffix appended after the region token for the output file name
OUTPUT_SUFFIX = "_merged.tif"

# Command name or path to gdalwarp
GDALWARP = "gdalwarp"

# Resampling mode passed to gdalwarp (near, bilinear, cubic, ...)
RESAMPLING = "near"

# Optional nodata value to apply to both source and destination (set to None to skip)
SRC_NODATA: Optional[str] = None

# Additional GDAL creation options (each entry should be like "KEY=VALUE")
CREATION_OPTIONS: List[str] = []

# Apply default creation options (COMPRESS=DEFLATE, TILED=YES, BIGTIFF=IF_SAFER)
USE_DEFAULT_CO = True

# If a region only has one tile, skip copying it to the output folder
SKIP_SINGLE = False

# Overwrite an existing output file for a region
OVERWRITE = False

# List planned merges without running gdalwarp or copying files
DRY_RUN = False

# File extensions to include
EXTENSIONS = [".tif", ".tiff"]

###############################################################################
# INTERNALS
###############################################################################

DEFAULT_CREATION_OPTS = ["COMPRESS=DEFLATE", "TILED=YES", "BIGTIFF=IF_SAFER"]


def build_creation_options(use_defaults: bool, extras: Iterable[str]) -> List[str]:
    options: List[str] = list(DEFAULT_CREATION_OPTS) if use_defaults else []
    for opt in extras:
        options.append(opt)
    return options


def is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def extract_region(stem: str, prefix: str, separators: str) -> Optional[str]:
    if prefix and not stem.startswith(prefix):
        return None
    trimmed = stem[len(prefix):] if prefix else stem
    sep_re = f"[{re.escape(separators)}]"
    tokens = re.split(sep_re, trimmed, maxsplit=1)
    if len(tokens) < 2:
        return None
    region = tokens[0].strip()
    return region or None


def find_tiles(
    input_dir: Path, output_dir: Path, prefix: str, separators: str, extensions: Iterable[str]
) -> Dict[str, List[Path]]:
    groups: Dict[str, List[Path]] = {}
    ext_set = {ext.lower() if ext.startswith(".") else f".{ext.lower()}" for ext in extensions}
    for path in input_dir.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() not in ext_set:
            continue
        if is_relative_to(path, output_dir):
            continue
        region = extract_region(path.stem, prefix, separators)
        if not region:
            continue
        groups.setdefault(region, []).append(path)
    return groups


def run_merge(
    gdalwarp_cmd: str,
    inputs: List[Path],
    output: Path,
    resampling: str,
    src_nodata: Optional[str],
    creation_options: List[str],
) -> int:
    cmd = [
        gdalwarp_cmd,
        "-overwrite",
        "-multi",
        "-r",
        resampling,
    ]
    if src_nodata:
        cmd.extend(["-srcnodata", src_nodata, "-dstnodata", src_nodata])
    for opt in creation_options:
        cmd.extend(["-co", opt])
    cmd.extend(str(p) for p in inputs)
    cmd.append(str(output))

    print(f"[CMD] {' '.join(cmd)}")
    result = subprocess.run(cmd)
    return result.returncode


def copy_single(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    print(f"[COPY] {src} -> {dst}")


def main() -> int:
    input_dir = Path(INPUT_FOLDER).expanduser()
    output_dir = Path(OUTPUT_FOLDER).expanduser()

    if not input_dir.is_dir():
        print(f"[ERROR] Input folder not found: {input_dir}")
        return 1

    if shutil.which(GDALWARP) is None:
        print(f"[ERROR] gdalwarp not found on PATH (looked for '{GDALWARP}').")
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)
    creation_options = build_creation_options(USE_DEFAULT_CO, CREATION_OPTIONS)

    groups = find_tiles(input_dir, output_dir, PREFIX, SEPARATORS, EXTENSIONS)
    if not groups:
        print("[INFO] No matching tiles found.")
        return 0

    for region, files in sorted(groups.items()):
        files = sorted(files)
        out_name = f"{PREFIX}{region}{OUTPUT_SUFFIX}"
        out_path = output_dir / out_name

        if out_path.exists() and not OVERWRITE:
            print(f"[SKIP] {out_path} already exists. Set OVERWRITE=True to rebuild.")
            continue

        if len(files) == 1:
            if SKIP_SINGLE:
                print(f"[SKIP] Only one tile for region '{region}'.")
                continue
            if DRY_RUN:
                print(f"[DRY] Would copy single tile for '{region}' -> {out_path}")
                continue
            copy_single(files[0], out_path)
            continue

        print(f"[MERGE] Region '{region}' with {len(files)} tiles.")
        if DRY_RUN:
            for f in files:
                print(f"       - {f}")
            print(f"       -> {out_path}")
            continue

        rc = run_merge(GDALWARP, files, out_path, RESAMPLING, SRC_NODATA, creation_options)
        if rc != 0:
            print(f"[ERROR] gdalwarp failed for region '{region}' (exit code {rc}).")
            return rc

    print("[DONE] Merging complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
