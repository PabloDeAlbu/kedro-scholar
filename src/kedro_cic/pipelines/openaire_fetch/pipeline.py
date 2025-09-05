from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    openaire_fetch_researchproduct,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="openaire_fetch_researchproduct",
            func=openaire_fetch_researchproduct,
            inputs=[
                "params:openaire_fetch_options.filter_param",
                "params:openaire_fetch_options.filter_value",
                "params:openaire_fetch_options.access_token",
                "params:openaire_fetch_options.refresh_token",
                "params:fetch_options.env",
            ],
            outputs=[
                "raw/openaire/researchproduct#parquet",
                "raw/openaire/researchproduct_dev#parquet",
                ],
        )
    ],
    tags="openaire_fetch"
    )
