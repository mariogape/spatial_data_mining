#!/usr/bin/env python3
"""
Plug-and-play script to clip a raster to every region shapefile in a folder.
Update the USER SETTINGS section, then run:

    python processing/mask_raster_by_regions.py

Requires GDAL's gdalwarp on PATH (from OSGeo4W, QGIS, etc.).
"""

import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List, Optional

###############################################################################
# USER SETTINGS - EDIT THESE
###############################################################################

# Raster that will be masked for each region
INPUT_RASTER = r"D:\Spatial Data\Land Cover\2023\landcover_clc.plus_f_30m_0..0cm_20220101_20241231_eu_epsg.3035_v20250327.tif"

# Folder containing one shapefile per region (only .shp files are used)
REGIONS_DIR = r"C:\Darwin geospatial\25.01 OpenPAS\AOI Shapefiles"

# Output folder where masked rasters will be written
OUTPUT_DIR = r"D:\OpenPas Spatial Data\Land Cover\2023"

# Suffix appended to each output file name (prefix comes from the shapefile stem)
OUTPUT_SUFFIX = "_mask.tif"

# Command name or path to gdalwarp
GDALWARP = "gdalwarp"

# Optional nodata value to burn outside the cutline (set to None to skip)
DST_NODATA: Optional[str] = "-9999"

# Add an alpha band to encode the mask (set to True if you prefer transparency)
ADD_ALPHA = False

# Additional GDAL creation options (each entry like "KEY=VALUE")
CREATION_OPTIONS: List[str] = ["COMPRESS=DEFLATE", "TILED=YES", "BIGTIFF=IF_SAFER"]

# Overwrite an existing output file for a region
OVERWRITE = True

###############################################################################
# INTERNALS
###############################################################################


def iter_shapefiles(folder: Path) -> List[Path]:
    return sorted([p for p in folder.glob("*.shp") if p.is_file()])


def build_creation_options(options: Iterable[str]) -> List[str]:
    result: List[str] = []
    for opt in options:
        if opt:
            result.append(opt)
    return result


def run_mask(
    gdalwarp_cmd: str,
    raster: Path,
    region: Path,
    output: Path,
    dst_nodata: Optional[str],
    add_alpha: bool,
    creation_options: List[str],
    overwrite: bool,
) -> int:
    cmd = [
        gdalwarp_cmd,
        "-cutline",
        str(region),
        "-crop_to_cutline",
    ]
    if overwrite:
        cmd.append("-overwrite")
    if add_alpha:
        cmd.append("-dstalpha")
    if dst_nodata:
        cmd.extend(["-dstnodata", dst_nodata])
    for opt in creation_options:
        cmd.extend(["-co", opt])
    cmd.append(str(raster))
    cmd.append(str(output))

    print(f"[CMD] {' '.join(cmd)}")
    result = subprocess.run(cmd)
    return result.returncode


def main() -> int:
    input_raster = Path(INPUT_RASTER)
    regions_dir = Path(REGIONS_DIR)
    output_dir = Path(OUTPUT_DIR)

    if not input_raster.is_file():
        print(f"[ERROR] Input raster not found: {input_raster}")
        return 1

    if not regions_dir.is_dir():
        print(f"[ERROR] Regions folder not found: {regions_dir}")
        return 1

    if shutil.which(GDALWARP) is None:
        print(f"[ERROR] gdalwarp not found on PATH (looked for '{GDALWARP}').")
        return 1

    shapefiles = iter_shapefiles(regions_dir)
    if not shapefiles:
        print(f"[INFO] No shapefiles found in {regions_dir}")
        return 0

    output_dir.mkdir(parents=True, exist_ok=True)
    creation_options = build_creation_options(CREATION_OPTIONS)

    for shp in shapefiles:
        out_name = f"{shp.stem}{OUTPUT_SUFFIX}"
        out_path = output_dir / out_name

        if out_path.exists() and not OVERWRITE:
            print(f"[SKIP] {out_path} already exists. Set OVERWRITE=True to rebuild.")
            continue

        print(f"[MASK] {shp.name} -> {out_path.name}")
        rc = run_mask(
            GDALWARP,
            input_raster,
            shp,
            out_path,
            DST_NODATA,
            ADD_ALPHA,
            creation_options,
            OVERWRITE,
        )
        if rc != 0:
            print(f"[ERROR] gdalwarp failed for {shp.name} (exit code {rc}).")
            return rc

    print("[DONE] Masking complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
