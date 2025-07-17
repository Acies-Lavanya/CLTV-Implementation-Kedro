from kedro.pipeline import Pipeline, node, pipeline
from cltv_implementation.nodes.preprocess import (
    preprocess_transactional,
    preprocess_behavioral,
    preprocess_customer,
)

def create_pipeline(selected_tables: list[str] = None) -> Pipeline:
    selected_tables = selected_tables or ["transactional"]  # default if none given

    pipeline_nodes = []

    if "transactional" in selected_tables:
        pipeline_nodes.append(
            node(preprocess_transactional, inputs="raw_transactional", outputs="preprocessed_transactional", name="preprocess_transactional_node")
        )

    if "customer" in selected_tables:
        pipeline_nodes.append(
            node(preprocess_customer, inputs="raw_customer", outputs="preprocessed_customer", name="preprocess_customer_node")
        )

    if "behavioral" in selected_tables:
        pipeline_nodes.append(
            node(preprocess_behavioral, inputs="raw_behavioral", outputs="preprocessed_behavioral", name="preprocess_behavioral_node")
        )

    return pipeline(pipeline_nodes)
