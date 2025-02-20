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
            inputs=[
                "params:openaire_fetch_options.filter_label",
                "raw/openaire/researchproduct#parquet",
            ],
            outputs=[
                "ldg/openaire/researchproduct",
                "ldg/openaire/map_researchproduct_originalId",
                "ldg/openaire/map_researchproduct_author",
                "ldg/openaire/map_researchproduct_subject",
                "ldg/openaire/map_researchproduct_pid",
                "ldg/openaire/map_researchproduct_url"
                ]
        ),
    ], tags="openaire_researchproduct")
