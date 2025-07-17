from kedro.pipeline import Pipeline
from .pipeline import create_pipeline

__all__ = ["create_pipeline"]

def register_pipelines() -> dict[str, Pipeline]:
    return {
        "bg_nbd_model": create_pipeline(),
        "__default__": create_pipeline(),
    }
