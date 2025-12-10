#!/usr/bin/env python
"""
Streamlit UI to configure and run the pipeline without editing YAML.
Usage: streamlit run scripts/ui_runner.py
"""
from pathlib import Path
from typing import Any, Dict, List

import streamlit as st
import yaml

from spatial_data_mining.orchestrator import run_pipeline_from_dict


def load_defaults():
    base_path = Path("config/base.yaml")
    if base_path.exists():
        with base_path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    else:
        data = {}
    defaults = data.get("defaults", {})
    allowed_crs = defaults.get("allowed_crs", ["EPSG:4326"])
    resolution_m = defaults.get("resolution_m", 20)
    storage_default = defaults.get("storage", {"kind": "local_cog", "output_dir": "data/outputs"})
    return allowed_crs, resolution_m, storage_default


def list_aois() -> List[str]:
    aoi_dir = Path("data/aoi")
    if not aoi_dir.exists():
        return []
    return [str(p) for p in aoi_dir.glob("*") if p.is_file()]


def main():
    st.set_page_config(page_title="Spatial ETL UI", layout="centered")
    st.title("Spatial Data Mining ETL")
    st.caption("Configure and run jobs without editing YAML.")

    st.info(
        "Prereqs: venv activated, dependencies installed, `earthengine authenticate` done. "
        "For GCS uploads, ensure ADC (`gcloud auth application-default login`) or set "
        "`GOOGLE_APPLICATION_CREDENTIALS`."
    )

    allowed_crs, default_res, storage_default = load_defaults()
    aoi_options = list_aois()

    with st.form("job_form"):
        name = st.text_input("Job name", value="ui_job")

        aoi_choice = st.selectbox(
            "AOI",
            options=["<custom path>"] + aoi_options,
            index=1 if aoi_options else 0,
            help="Files from data/aoi/ or enter a custom path.",
        )
        if aoi_choice == "<custom path>":
            aoi_path = st.text_input("Custom AOI path", value="data/aoi/sample.geojson")
        else:
            aoi_path = aoi_choice

        target_crs = st.selectbox("Target CRS", options=allowed_crs, index=0)
        resolution_m = st.number_input("Resolution (meters)", value=float(default_res), min_value=1.0)
        year = st.number_input("Year", value=2023, step=1, format="%d")
        season = st.text_input("Season", value="summer")

        variables = st.multiselect(
            "Variables",
            options=["ndvi", "ndmi", "msi"],
            default=["ndvi", "ndmi", "msi"],
            help="Choose one or more indices to process.",
        )

        storage_kind = st.selectbox("Storage kind", options=["local_cog", "gcs_cog"], index=0)
        if storage_kind == "local_cog":
            output_dir = st.text_input("Local output dir", value=storage_default.get("output_dir", "data/outputs"))
            bucket = None
            prefix = None
        else:
            output_dir = storage_default.get("output_dir", "data/outputs")
            bucket = st.text_input("GCS bucket", value=storage_default.get("bucket", "your-bucket"))
            prefix = st.text_input("GCS prefix (optional)", value=storage_default.get("prefix", "spatial/outputs"))

        submitted = st.form_submit_button("Run pipeline")

    if submitted:
        if not variables:
            st.error("Select at least one variable.")
            return
        job_section: Dict[str, Any] = {
            "name": name,
            "aoi_path": aoi_path,
            "target_crs": target_crs,
            "resolution_m": resolution_m,
            "year": int(year),
            "season": season,
            "variables": variables,
            "storage": {"kind": storage_kind, "output_dir": output_dir},
        }
        if storage_kind == "gcs_cog":
            job_section["storage"]["bucket"] = bucket
            job_section["storage"]["prefix"] = prefix

        with st.spinner("Running pipeline..."):
            try:
                results = run_pipeline_from_dict(job_section)
            except Exception as exc:  # noqa: BLE001
                st.error(f"Pipeline failed: {exc}")
                return

        st.success("Pipeline completed.")
        for res in results:
            st.write(f"- {res['variable']}: local={res['local_path']}, gcs={res['gcs_uri']}")


if __name__ == "__main__":
    main()
