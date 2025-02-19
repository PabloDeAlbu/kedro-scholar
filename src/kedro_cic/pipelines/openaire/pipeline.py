from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    fetch_openaire_researchproduct,
    land_openaire_researchproduct,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="fetch_openaire_researchproduct",
            func=fetch_openaire_researchproduct,
            inputs=[
                "params:openaire_fetch_options.filter_label",
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
        ),
        node(
            name="land_openaire_researchproduct",
            func=land_openaire_researchproduct,
            inputs="raw/openaire/researchproduct#parquet",
            outputs=[
                "ldg/openaire/researchproduct",
                "ldg/openaire/researchproduct2originalId",
                "ldg/openaire/researchproduct2author",
                "ldg/openaire/researchproduct2subject",
                "ldg/openaire/researchproduct2pid",
                "ldg/openaire/researchproduct2url"
                ]
        ),
    ], tags="openaire_researchproduct")
