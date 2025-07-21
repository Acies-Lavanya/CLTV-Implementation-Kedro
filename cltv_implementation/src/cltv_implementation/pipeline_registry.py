from kedro.pipeline import Pipeline
from kedro.config import OmegaConfigLoader
from cltv_implementation.pipelines.preprocess.pipeline import create_pipeline as create_preprocessing_pipeline
from cltv_implementation.pipelines.feature_engineering.pipeline import create_pipeline as create_user_level_pipeline
from cltv_implementation.pipelines.bg_nbd.pipeline import create_pipeline as create_bg_nbd_table
def register_pipelines():
    config_loader = OmegaConfigLoader(conf_source="conf")
    parameters = config_loader.get("parameters*") or {}
    selected_tables = parameters.get("selected_tables")
    print("📌 DEBUG: Selected tables from parameters:", selected_tables)

    # Always include transactional preprocess
    preprocessing = create_preprocessing_pipeline()

    # Always include user level merge
    user_level = create_user_level_pipeline()

    model_level = create_bg_nbd_table()
    return {
        "__default__": preprocessing + model_level + user_level,
        "preprocessing_only": preprocessing,
        "user_level_only": user_level,
        "model_only": model_level,
    }
