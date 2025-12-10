# Spatial Data Mining ETL

Config-driven geospatial ETL for extracting indices from Google Earth Engine (and future sources), transforming to analysis-ready rasters, and exporting as Cloud-Optimized GeoTIFFs (COGs) locally or to Google Cloud Storage (GCS).

## Environment setup
- Prereqs: Python 3.10+ recommended; `gcloud` only if you plan to upload to GCS.
- Create/activate venv and install deps:
  - Unix/macOS: `./scripts/setup_env.sh && source .venv/bin/activate`
  - Windows (PowerShell): `python -m venv .venv; .\\.venv\\Scripts\\Activate.ps1; pip install -r requirements.txt`
- Authenticate Google Earth Engine (first time): `earthengine authenticate`
- Optional GCS auth: `gcloud auth application-default login` or set `GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json`

## Quickstart (UI-driven)
- Prereqs: Python 3.10+ (recommended), `earthengine-api`, `streamlit`; optional `gcloud` for GCS uploads.
- Setup: `./scripts/setup_env.sh` then activate venv (`source .venv/bin/activate` on Unix/macOS, `.\\.venv\\Scripts\\Activate.ps1` on Windows).
- Authenticate GEE (first time): `earthengine authenticate`.
- Optional GCS auth: `gcloud auth application-default login` or set `GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json`.
- Prepare AOI: place your AOI file in `data/aoi/` (GeoJSON or Shapefile with a defined CRS).
- Launch UI: `streamlit run scripts/ui_runner.py`
- In the browser, select AOI, CRS, resolution, year/season, variables (ndvi/ndmi/msi), storage (local/GCS), then click “Run pipeline.”
- Outputs: COGs written locally to `data/outputs/` (and uploaded to GCS if selected).

## How to use the pipeline (step-by-step)
1) Environment: run `./scripts/setup_env.sh` and activate the venv.  
2) Auth: `earthengine authenticate`; for GCS uploads, ensure ADC via `gcloud auth application-default login` or `GOOGLE_APPLICATION_CREDENTIALS`.  
3) AOI: drop a GeoJSON/Shapefile with CRS into `data/aoi/`.  
4) Run UI: `streamlit run scripts/ui_runner.py`  
5) Configure in UI:
   - Pick AOI from the dropdown (or enter a custom path).  
   - Choose CRS (EPSG:3035, 4326, 3857, 25829, 25830, 25831) and resolution (m).  
   - Set year and season (used for filtering and naming).  
   - Select variables: ndvi, ndmi, msi.  
   - Choose storage: local COG (with output dir) or GCS (bucket/prefix).  
6) Execute: click “Run pipeline.” The UI shows progress and outputs.  
7) Verify outputs:
   - Local: `data/outputs/<name>_<variable>_<year>_<season>_<crs>.tif`
   - GCS: `gs://<bucket>/<prefix>/...` if enabled.

## What you configure (in the UI)
- AOI file: pick from `data/aoi/` or enter a path (GeoJSON/Shapefile; CRS auto-detected).
- CRS: choose EPSG:3035, 4326, 3857, 25829, 25830, 25831.
- Resolution: target pixel size in meters.
- Time: year and season (for filtering and naming).
- Variables: any of ndvi, ndmi, msi.
- Storage: local COG output dir, or GCS bucket/prefix if uploading.

## Repo structure (key folders)
- `scripts/` – setup script and Streamlit UI runner.
- `config/` – defaults (`base.yaml`) and per-job configs (`jobs/*.yaml`).
- `src/spatial_data_mining/` – library code: orchestrator, extractors, transforms, loaders, utils, variable registry.
- `data/` – AOIs (`aoi/`) and outputs (`outputs/`).
- `legacy/` – existing notebooks/scripts parked here for reference.

## Orchestration flow
1) Load/validate config (merge `base.yaml` + job YAML).  
2) Load AOI, detect CRS, reproject to target CRS.  
3) For each variable: resolve extractor+transform chain → extract from source (GEE for now) → clip/reproject/resample.  
4) Write COG locally; if `storage.kind=gcs_cog`, upload to GCS.  
5) Log progress and summary.

## Adding a variable
1) Implement variable definition in `variables/registry.py` (map name → extractor args + transform chain).  
2) If new source needed, add an extractor module under `extract/` and reference it in the registry.  
3) Optionally add tests in `tests/`.

## Notes
- COG writing uses deflate compression and overviews.
- Keep AOIs small enough for GEE exports; consider tiling for very large areas (future enhancement).
- Seasonal filtering is placeholder; refine date filters as needed.
