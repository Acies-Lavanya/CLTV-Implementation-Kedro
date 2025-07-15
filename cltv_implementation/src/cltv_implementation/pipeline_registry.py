from kedro.pipeline import Pipeline
from cltv_implementation.pipelines.preprocess import pipeline as preprocess_pipeline

def register_pipelines() -> dict[str, Pipeline]:
    return {
        "__default__": preprocess_pipeline.create_pipeline()
    }
