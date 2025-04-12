from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    fetch_openaire_researchproduct,
    land_openaire_rel_researchproduct_author,
    land_openaire_rel_researchproduct_instance,
    land_openaire_rel_researchproduct_pid,
    land_openaire_rel_researchproduct_subject,
    land_openaire_rel_researchproduct_originalid,
    land_openaire_researchproduct,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="fetch_openaire_researchproduct",
            func=fetch_openaire_researchproduct,
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
        ),
        node(
            name="land_openaire_rel_researchproduct_author",
            func=land_openaire_rel_researchproduct_author,
            inputs="raw/openaire/researchproduct#parquet",
            outputs="ldg/openaire/rel_researchproduct_author",
        ),
        # node(
        #     name="land_openaire_rel_researchproduct_instance",
        #     func=land_openaire_rel_researchproduct_instance,
        #     inputs="raw/openaire/researchproduct#parquet",
        #     outputs=[
        #         "ldg/openaire/rel_researchproduct_instance",
        #         "ldg/openaire/rel_researchproduct_url",
        #         "ldg/openaire/rel_researchproduct_alternateidentifier",
        #     ]
        # ),
        node(
            name="land_openaire_rel_researchproduct_originalid",
            func=land_openaire_rel_researchproduct_originalid,
            inputs="raw/openaire/researchproduct#parquet",
            outputs="ldg/openaire/rel_researchproduct_originalid"
        ),
        node(
            name="land_openaire_rel_researchproduct_pid",
            func=land_openaire_rel_researchproduct_pid,
            inputs="raw/openaire/researchproduct#parquet",
            outputs="ldg/openaire/rel_researchproduct_pid"
        ),
        node(
            name="land_openaire_rel_researchproduct_subject",
            func=land_openaire_rel_researchproduct_subject,
            inputs="raw/openaire/researchproduct#parquet",
            outputs="ldg/openaire/rel_researchproduct_subject"
        ),
        node(
            name="land_openaire_researchproduct",
            func=land_openaire_researchproduct,
            inputs=[
                "params:openaire_fetch_options.filter_param",
                "params:openaire_fetch_options.filter_value",
                "raw/openaire/researchproduct#parquet",
            ],
            outputs="ldg/openaire/researchproduct"
        ),
    ], tags="openaire_researchproduct")
