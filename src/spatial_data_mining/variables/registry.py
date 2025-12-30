from spatial_data_mining.extract.alpha_earth import AlphaEarthExtractor
from spatial_data_mining.extract.clcplus import CLCPlusExtractor
from spatial_data_mining.extract.gee import GEEExtractor
from spatial_data_mining.transform.raster_ops import (
    process_clcplus_to_target,
    process_raster_to_target,
)

VARIABLES = {
    "ndvi": {"extractor_factory": lambda _job=None: GEEExtractor("NDVI"), "transform": process_raster_to_target},
    "ndmi": {"extractor_factory": lambda _job=None: GEEExtractor("NDMI"), "transform": process_raster_to_target},
    "msi": {"extractor_factory": lambda _job=None: GEEExtractor("MSI"), "transform": process_raster_to_target},
    "bsi": {"extractor_factory": lambda _job=None: GEEExtractor("BSI"), "transform": process_raster_to_target},
    "alpha_earth": {
        "extractor_factory": lambda _job=None: AlphaEarthExtractor(),
        "transform": process_raster_to_target,
    },
    "clcplus": {
        "extractor_factory": lambda job=None: CLCPlusExtractor(
            getattr(job, "clcplus_input_dir", None)
        ),
        "transform": process_clcplus_to_target,
    },
}


def _resolve_extractor(var_def: dict, job_cfg=None):
    if "extractor_factory" in var_def:
        return var_def["extractor_factory"](job_cfg)
    return var_def.get("extractor")


def get_variable(name: str, job_cfg=None):
    key = name.lower()
    if key not in VARIABLES:
        raise KeyError(f"Variable not registered: {name}")
    var_def = VARIABLES[key]
    extractor = _resolve_extractor(var_def, job_cfg=job_cfg)
    return {"extractor": extractor, "transform": var_def["transform"]}
