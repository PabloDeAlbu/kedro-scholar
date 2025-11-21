from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    openaire_extract_researchproduct,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="openaire_extract_researchproduct",
            func=openaire_extract_researchproduct,
            inputs=[
                "params:openaire_extract_options.filter_param",
                "params:openaire_extract_options.ror_filter_value",
                "params:openaire_extract_options.access_token",
                "params:openaire_extract_options.refresh_token",
                "params:fetch_options.env",
            ],
            outputs=[
                "raw/openaire/researchproduct#parquet",
                "raw/openaire/researchproduct_dev#parquet",
                ],
        )
    ],
    tags="openaire_extract"
    )
