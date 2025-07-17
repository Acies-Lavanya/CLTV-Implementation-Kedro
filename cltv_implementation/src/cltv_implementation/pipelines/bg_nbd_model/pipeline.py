from kedro.pipeline import Pipeline, node, pipeline
from cltv_implementation.nodes.bg_nbd_model import train_bg_nbd_model

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=train_bg_nbd_model,
            inputs="transactions_df",
            outputs="bg_nbd_cltv_output",
            name="train_bg_nbd_model_node"
        ),
    ])
