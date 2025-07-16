from kedro.pipeline import Pipeline, node, pipeline
from cltv_implementation.nodes.feature_engineering import calculate_rfm

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=calculate_rfm,
            inputs="preprocessed_transactional",
            outputs="rfm_output",
            name="rfm_node"
        )
    ])
