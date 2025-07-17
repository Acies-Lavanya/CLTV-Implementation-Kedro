from __future__ import annotations
import os, json
from kedro.pipeline import Pipeline

# 1️⃣  Import the pipeline‑factory functions (not the modules)
from cltv_implementation.pipelines.preprocess.pipeline import create_pipeline as create_preprocess_pipeline
from cltv_implementation.pipelines.feature_engineering.pipeline import create_pipeline as create_rfm_pipeline

def register_pipelines() -> dict[str, Pipeline]:
    # ── Read selected tables from env var ──────────────────────────────
    params = json.loads(os.getenv("KEDRO_PARAMS", "{}"))
    selected_tables = params.get("selected_tables", [])

    # ── Build pipelines list ───────────────────────────────────────────
    pipelines: list[Pipeline] = []

    # transactional is mandatory; build preprocess pipeline with selector
    pipelines.append(create_preprocess_pipeline(selected_tables=selected_tables))

    # RFM pipeline always runs
    pipelines.append(create_rfm_pipeline())

    # ── Combine and return ─────────────────────────────────────────────
    master_pipeline = sum(pipelines, Pipeline([]))
    return {"__default__": master_pipeline}

