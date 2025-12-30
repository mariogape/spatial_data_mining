VARIABLE_METADATA = {
    "ndvi": {
        "source": "COPERNICUS/S2_SR_HARMONIZED",
        "native_resolution_m": 10,
        "calculation": "normalizedDifference(['B8','B4'])",
        "notes": "Uses Sentinel-2 NIR (B8) and red (B4) 10m bands.",
        "temporal_coverage": {"start_year": 2017, "end_year": "present"},
        "temporal_resolution": "5_day",
        "season_options": ["winter", "spring", "summer", "autumn", "annual"],
    },
    "ndmi": {
        "source": "COPERNICUS/S2_SR_HARMONIZED",
        "native_resolution_m": 20,
        "calculation": "normalizedDifference(['B8','B11'])",
        "notes": "Uses Sentinel-2 NIR (10m) and SWIR1 (20m); native grid is 20m.",
        "temporal_coverage": {"start_year": 2017, "end_year": "present"},
        "temporal_resolution": "5_day",
        "season_options": ["winter", "spring", "summer", "autumn", "annual"],
    },
    "msi": {
        "source": "COPERNICUS/S2_SR_HARMONIZED",
        "native_resolution_m": 20,
        "calculation": "B11 / B8",
        "notes": "Uses Sentinel-2 SWIR1 (20m) over NIR (10m); native grid is 20m.",
        "temporal_coverage": {"start_year": 2017, "end_year": "present"},
        "temporal_resolution": "5_day",
        "season_options": ["winter", "spring", "summer", "autumn", "annual"],
    },
    "bsi": {
        "source": "COPERNICUS/S2_SR_HARMONIZED",
        "native_resolution_m": 20,
        "calculation": "((B11 + B4) - (B8 + B2)) / ((B11 + B4) + (B8 + B2))",
        "notes": "Bare Soil Index (BSI) using Sentinel-2 SWIR1 (B11), red (B4), NIR (B8), blue (B2); output grid is 20m.",
        "temporal_coverage": {"start_year": 2017, "end_year": "present"},
        "temporal_resolution": "5_day",
        "season_options": ["winter", "spring", "summer", "autumn", "annual"],
    },
    "alpha_earth": {
        "source": "GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL",
        "native_resolution_m": 10,
        "calculation": "Annual Alpha Earth embedding raster",
        "notes": "Annual embeddings produced by Google AlphaEarth; single yearly composite.",
        "temporal_coverage": {"start_year": 2017, "end_year": "present"},
        "temporal_resolution": "annual",
        "season_options": ["static"],
    },
    "clcplus": {
        "source": "CLCplus (user-provided, one raster per country)",
        "native_resolution_m": 10,
        "calculation": "Manual ingestion of provided CLCplus classification raster",
        "notes": "Provide the local CLCplus folder; values 0 and nodata are remapped to -9999.",
        "temporal_coverage": {"start_year": 2018, "end_year": 2023},
        "temporal_resolution": "static",
        "season_options": ["static"],
    },
}


def get_variable_metadata(name: str) -> dict:
    key = name.lower()
    return VARIABLE_METADATA.get(key, {})
