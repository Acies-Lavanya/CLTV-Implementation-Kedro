from cltv_implementation.nodes.model import train_bg_nbd_model
from kedro.pipeline import Pipeline, node, pipeline

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=train_bg_nbd_model,
            inputs="preprocessed_transactional",
            outputs="transaction_with_bgnbd",
            name="train_bg_nbd_model_node"
        ),
    ])

