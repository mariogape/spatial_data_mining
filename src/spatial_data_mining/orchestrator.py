import logging
import re
import tempfile
import uuid
from pathlib import Path
from typing import List, Dict, Any, Callable, Optional

from spatial_data_mining.config import (
    load_job_config,
    load_job_config_from_dict,
)
from spatial_data_mining.load.cog import write_cog
from spatial_data_mining.load.gcs import upload_to_gcs
from spatial_data_mining.utils.aoi import load_aoi, get_aoi_geometries
from spatial_data_mining.utils.logging import setup_logging
from spatial_data_mining.variables.registry import get_variable
from spatial_data_mining.utils.cancellation import check_cancelled, PipelineCancelled

ProgressCB = Optional[Callable[[str], None]]


def _notify(cb: ProgressCB, message: str) -> None:
    if cb:
        try:
            cb(message)
        except Exception as exc:  # never fail the pipeline due to UI/logging issues
            logging.getLogger(__name__).warning("Progress callback failed: %s", exc)


def _slugify_name(name: str) -> str:
    """Lowercase slug for filenames; collapse non-alnum to underscores."""
    slug = re.sub(r"[^A-Za-z0-9]+", "_", str(name)).strip("_").lower()
    return slug or "aoi"


def _run(
    job_cfg,
    logging_cfg,
    progress_cb: ProgressCB = None,
    should_stop: Optional[Callable[[], bool]] = None,
) -> List[Dict[str, Any]]:
    setup_logging(logging_cfg)
    logger = logging.getLogger("orchestrator")
    project_root = Path(__file__).resolve().parents[2]

    def _check_stop():
        check_cancelled(should_stop)

    logger.info("Loaded job: %s", job_cfg.name)
    _notify(progress_cb, f"Loaded job: {job_cfg.name}")
    _check_stop()

    output_dir = Path(job_cfg.storage.output_dir or (project_root / "data/outputs"))
    if not output_dir.is_absolute():
        output_dir = project_root / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    _check_stop()

    results: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    years_val = getattr(job_cfg, "years", None)
    seasons_val = getattr(job_cfg, "seasons", None)
    aois_val = getattr(job_cfg, "aoi_paths", None)

    years = years_val or ([] if getattr(job_cfg, "year", None) is None else [job_cfg.year])
    seasons = seasons_val or ([] if getattr(job_cfg, "season", None) is None else [job_cfg.season])
    aois = aois_val or ([] if getattr(job_cfg, "aoi_path", None) is None else [job_cfg.aoi_path])
    var_slug_map: Dict[str, str] = {}

    try:
        for aoi_path in aois:
            _check_stop()
            try:
                aoi_gdf = load_aoi(aoi_path)
                geom_wgs84, geom_target = get_aoi_geometries(aoi_gdf, job_cfg.target_crs)
                aoi_slug = _slugify_name(Path(aoi_path).stem)
                _notify(progress_cb, f"AOI loaded and reprojected: {aoi_slug}")
            except PipelineCancelled:
                raise
            except Exception as exc:
                logger.exception("Skipping AOI %s due to error", aoi_path)
                _notify(progress_cb, f"Skipping AOI {aoi_path}: {exc}")
                errors.append({"aoi_path": str(aoi_path), "error": str(exc)})
                continue

            for season in seasons:
                _check_stop()
                season_slug = _slugify_name(season)
                for year in years:
                    _check_stop()
                    for var_name in job_cfg.variables:
                        _check_stop()
                        if var_name not in var_slug_map:
                            var_slug_map[var_name] = _slugify_name(var_name)
                        var_slug = var_slug_map[var_name]
                        logger.info(
                            "Processing variable %s for year %s season %s (AOI %s)",
                            var_name,
                            year,
                            season,
                            aoi_slug,
                        )
                        _notify(
                            progress_cb,
                            f"Processing {var_name} ({year}, {season}) for {aoi_slug}...",
                        )
                        var_def = get_variable(var_name, job_cfg=job_cfg)
                        extractor = var_def["extractor"]
                        transform_fn = var_def["transform"]

                        try:
                            # On Windows, cleanup can fail intermittently due to lingering file handles.
                            # Cleanup errors should not cause a successful variable run to be reported as failed.
                            with tempfile.TemporaryDirectory(
                                dir=output_dir, ignore_cleanup_errors=True
                            ) as tmp_dir:
                                raw_result = extractor.extract(
                                    aoi_geojson=geom_wgs84,
                                    year=year,
                                    season=season,
                                    resolution_m=job_cfg.resolution_m,
                                    temp_dir=tmp_dir,
                                    progress_cb=progress_cb,
                                    should_stop=should_stop,
                                )
                                # Allow extractors to optionally return (path, effective_resolution_m)
                                if isinstance(raw_result, tuple):
                                    raw_path, effective_res = raw_result
                                else:
                                    raw_path, effective_res = raw_result, job_cfg.resolution_m
                                _notify(
                                    progress_cb,
                                    f"{var_name} ({year}, {season}) {aoi_slug}: downloaded raw image {raw_path}",
                                )
                                _check_stop()

                                processed_path = transform_fn(
                                    src_path=raw_path,
                                    target_crs=job_cfg.target_crs,
                                    resolution_m=effective_res,
                                    aoi_geom_target=geom_target,
                                )
                                _notify(
                                    progress_cb,
                                    f"{var_name} ({year}, {season}) {aoi_slug}: transformed to target CRS/resolution",
                                )
                                _check_stop()

                                filename = f"{var_slug}_{year}_{season_slug}_{aoi_slug}.tif"
                                local_output = output_dir / filename
                                tmp_output = output_dir / f".{filename}.{uuid.uuid4().hex}.tmp.tif"

                                gcs_uri = None
                                try:
                                    write_cog(processed_path, tmp_output)
                                    _check_stop()

                                    if job_cfg.storage.kind == "gcs_cog":
                                        _check_stop()
                                        gcs_uri = upload_to_gcs(
                                            tmp_output, job_cfg.storage.bucket, job_cfg.storage.prefix
                                        )
                                        logger.info("Uploaded to GCS: %s", gcs_uri)
                                        _notify(
                                            progress_cb,
                                            f"{var_name} ({year}, {season}) {aoi_slug}: uploaded to {gcs_uri}",
                                        )
                                        _check_stop()

                                    # Finalize the local output only after all required steps succeed
                                    # (e.g., upload for gcs_cog), so incomplete runs don't leave outputs behind.
                                    tmp_output.replace(local_output)
                                finally:
                                    if tmp_output.exists():
                                        try:
                                            tmp_output.unlink()
                                        except Exception:
                                            pass

                                _notify(
                                    progress_cb,
                                    f"{var_name} ({year}, {season}) {aoi_slug}: wrote COG {local_output}",
                                )
                                _check_stop()

                                results.append(
                                    {
                                        "aoi": aoi_slug,
                                        "aoi_path": str(Path(aoi_path).resolve()),
                                        "variable": var_name,
                                        "year": year,
                                        "season": season,
                                        "local_path": str(local_output),
                                        "gcs_uri": gcs_uri,
                                    }
                                )
                                logger.info(
                                    "Finished variable %s for year %s season %s (AOI %s)",
                                    var_name,
                                    year,
                                    season,
                                    aoi_slug,
                                )
                                _notify(
                                    progress_cb,
                                    f"Finished {var_name} ({year}, {season}) for {aoi_slug}",
                                )
                        except PipelineCancelled:
                            logger.info("Pipeline cancelled by user.")
                            _notify(progress_cb, "Pipeline stopped by user.")
                            raise
                        except Exception as exc:
                            logger.exception(
                                "Failed variable %s for year %s season %s (AOI %s)",
                                var_name,
                                year,
                                season,
                                aoi_slug,
                            )
                            _notify(
                                progress_cb,
                                f"Failed {var_name} ({year}, {season}) for {aoi_slug}: {exc}",
                            )
                            errors.append(
                                {
                                    "aoi": aoi_slug,
                                    "aoi_path": str(aoi_path),
                                    "variable": var_name,
                                    "year": year,
                                    "season": season,
                                    "error": str(exc),
                                }
                            )
                            continue
    except PipelineCancelled:
        logger.info("Pipeline cancelled by user.")
        _notify(progress_cb, "Pipeline stopped by user.")
        raise

    if errors:
        logger.warning("Job %s completed with %d error(s).", job_cfg.name, len(errors))
        _notify(progress_cb, f"Job completed with {len(errors)} error(s); see logs for details.")
    else:
        logger.info("Job %s completed. Outputs: %s", job_cfg.name, results)
        _notify(progress_cb, "Job completed.")
    return results


def run_pipeline(
    config_path: str,
    progress_cb: ProgressCB = None,
    should_stop: Optional[Callable[[], bool]] = None,
    **_: Any,
) -> List[Dict[str, Any]]:
    job_cfg, logging_cfg = load_job_config(config_path)
    return _run(job_cfg, logging_cfg, progress_cb=progress_cb, should_stop=should_stop)


def run_pipeline_from_dict(
    job_section: Dict[str, Any],
    progress_cb: ProgressCB = None,
    should_stop: Optional[Callable[[], bool]] = None,
    **_: Any,
) -> List[Dict[str, Any]]:
    """
    Run pipeline directly from an in-memory job dict (no YAML needed).
    """
    job_cfg, logging_cfg = load_job_config_from_dict(job_section)
    return _run(job_cfg, logging_cfg, progress_cb=progress_cb, should_stop=should_stop)
