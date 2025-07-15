from kedro.pipeline import Pipeline, node
from cltv_implementation.nodes.preprocess import (
    preprocess_transactional,
    preprocess_customer,
    preprocess_behavioral,
)

def create_pipeline(**kwargs):
    return Pipeline([
        node(
            func=preprocess_transactional,
            inputs="raw_transactional",
            outputs="preprocessed_transactional",
            name="preprocess_transactional_node",
        ),
        node(
            func=preprocess_customer,
            inputs="raw_customer",
            outputs="preprocessed_customer",
            name="preprocess_customer_node",
        ),
        node(
            func=preprocess_behavioral,
            inputs="raw_behavioral",
            outputs="preprocessed_behavioral",
            name="preprocess_behavioral_node",
        ),
    ])
