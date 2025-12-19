import logging
from pathlib import Path
from typing import Any

import rioxarray
from rasterio.enums import Resampling
from rasterio.warp import calculate_default_transform
from shapely.geometry import mapping, box

logger = logging.getLogger(__name__)


def _normalize_spatial_dims(data):
    """
    Ensure rioxarray data uses x/y spatial dims even if named differently.
    """
    # Only squeeze non-spatial singleton dims (e.g., band), keep spatial dims even if size 1.
    if "band" in data.dims and data.sizes.get("band", 0) == 1:
        data = data.squeeze("band", drop=True)
    if "variable" in data.dims and data.sizes.get("variable", 0) == 1:
        data = data.squeeze("variable", drop=True)

    if "x" not in data.dims or "y" not in data.dims:
        spatial_dims = [d for d in data.dims if d not in ("band", "variable")]
        if len(spatial_dims) >= 2:
            y_dim, x_dim = spatial_dims[-2:]
        elif len(data.dims) >= 2:
            y_dim, x_dim = data.dims[-2:]
        else:
            raise ValueError("Could not infer spatial dimensions for raster.")

        rename_map = {}
        if x_dim != "x":
            rename_map[x_dim] = "x"
        if y_dim != "y":
            rename_map[y_dim] = "y"
        if rename_map:
            data = data.rename(rename_map)
    return data.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=False)


def _reproject_raster(data, target_crs: str, resolution_m: float | None, resampling: Resampling):
    reproject_kwargs = {
        "dst_crs": target_crs,
        "resampling": resampling,
    }
    if resolution_m is not None:
        reproject_kwargs["resolution"] = resolution_m
    else:
        # Preserve native pixel grid: compute destination transform/shape matching source pixel counts.
        try:
            with data.rio.env():
                transform, width, height = calculate_default_transform(
                    data.rio.crs,
                    target_crs,
                    data.rio.width,
                    data.rio.height,
                    *data.rio.bounds(),
                    dst_width=data.rio.width,
                    dst_height=data.rio.height,
                )
            reproject_kwargs["transform"] = transform
            reproject_kwargs["shape"] = (height, width)
        except Exception as exc:
            logger.warning("Falling back to default transform when preserving native grid: %s", exc)

    return data.rio.reproject(**reproject_kwargs)


def _clip_to_aoi(data, target_crs: str, aoi_geom_target: Any):
    aoi_geom = aoi_geom_target
    try:
        return data.rio.clip([mapping(aoi_geom)], target_crs, drop=True)
    except Exception as exc:
        logger.warning("Clip failed (%s); retrying with all_touched=True", exc)
        try:
            return data.rio.clip([mapping(aoi_geom)], target_crs, drop=True, all_touched=True)
        except Exception as exc2:
            try:
                raster_box = box(*data.rio.bounds())
                if not raster_box.intersects(aoi_geom):
                    raise
                logger.warning("Clip failed again (%s); writing un-clipped raster as fallback", exc2)
                return data
            except Exception:
                raise


def process_raster_to_target(
    src_path: Path,
    target_crs: str,
    resolution_m: float | None,
    aoi_geom_target: Any,
) -> Path:
    """
    Reproject, resample, and clip to the target AOI/CRS.
    Returns a path to a temporary GeoTIFF in the target CRS.
    """
    src_path = Path(src_path)
    processed_path = src_path.with_name(f"{src_path.stem}_processed.tif")

    data = rioxarray.open_rasterio(src_path, masked=True)
    data = _normalize_spatial_dims(data)
    data = _reproject_raster(data, target_crs, resolution_m, Resampling.bilinear)
    data = _clip_to_aoi(data, target_crs, aoi_geom_target)

    data.rio.to_raster(processed_path, compress="deflate")

    return processed_path


def process_clcplus_to_target(
    src_path: Path,
    target_crs: str,
    resolution_m: float | None,
    aoi_geom_target: Any,
) -> Path:
    """
    Reproject CLCplus rasters with nearest-neighbor resampling, clip to AOI,
    and recode 0 / nodata pixels to -999 before writing a GeoTIFF.
    """
    src_path = Path(src_path)
    processed_path = src_path.with_name(f"{src_path.stem}_processed.tif")

    data = rioxarray.open_rasterio(src_path, masked=True)
    data = _normalize_spatial_dims(data)
    data = _reproject_raster(data, target_crs, resolution_m, Resampling.nearest)
    data = _clip_to_aoi(data, target_crs, aoi_geom_target)

    # Recode nodata and zero values to -999 and preserve integer semantics.
    data = data.fillna(-999)
    data = data.where(data != 0, other=-999)
    try:
        data = data.astype("int32")
        data.rio.write_nodata(-999, inplace=True)
    except Exception as exc:  # best-effort typing/nodata; continue even if write_nodata fails
        logger.warning("Could not enforce nodata/-999 typing on CLCplus raster: %s", exc)

    data.rio.to_raster(processed_path, compress="deflate")

    return processed_path
