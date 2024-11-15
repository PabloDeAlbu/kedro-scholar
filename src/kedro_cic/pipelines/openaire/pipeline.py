from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    fetch_researchproduct_openaire,
    land_researchproduct_openaire,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="fetch_researchproduct_openaire",
            func=fetch_researchproduct_openaire,
            inputs=[
                "stg/dim_doi",
                "params:openaire_fetch_options.refresh_token",
                "params:fetch_options.env",
            ],
            outputs="raw/openaire/researchproduct",
        ),
        node(
            name="land_researchproduct_openaire",
            func=land_researchproduct_openaire,
            inputs=["raw/openaire/researchproduct"],
            outputs="ldg/openaire/researchproduct",
        ),
    ], tags="openaire"
)