from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    fetch_researchproduct_openaire,
    land_researchproduct_openaire,
    land_researchproduct2creator_openaire,
    land_researchproduct2measure_openaire,
    land_researchproduct2pid_openaire,
    land_researchproduct2relevantdate_openaire,
    land_researchproduct2subject_openaire,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="fetch_researchproduct_openaire",
            func=fetch_researchproduct_openaire,
            inputs=[
                "stg/dim_doi",
                #"stg_openaire_graph/hub_openaire_graph_originalid",
                "params:openaire_fetch_options.access_token",
                "params:fetch_options.env",
            ],
            outputs="raw/openaire/researchproduct",
        ),
       node(
            name="land_researchproduct_openaire",
            func=land_researchproduct_openaire,
            inputs="raw/openaire/researchproduct",
            outputs="ldg/openaire/researchproduct"
        ),
        node(
            name="land_researchproduct2creator_openaire",
            func=land_researchproduct2creator_openaire,
            inputs="raw/openaire/researchproduct",
            outputs="ldg/openaire/researchproduct2creator"
        ),
        node(
            name="land_researchproduct2measure_openaire",
            func=land_researchproduct2measure_openaire,
            inputs="raw/openaire/researchproduct",
            outputs="ldg/openaire/researchproduct2measure"
        ),
        node(
            name="land_researchproduct2pid_openaire",
            func=land_researchproduct2pid_openaire,
            inputs="raw/openaire/researchproduct",
            outputs="ldg/openaire/researchproduct2pid"
        ),
        node(
            name="land_researchproduct2relevantdate_openaire",
            func=land_researchproduct2relevantdate_openaire,
            inputs="raw/openaire/researchproduct",
            outputs="ldg/openaire/researchproduct2relevantdate"
        ),
        node(
            name="land_researchproduct2subject_openaire",
            func=land_researchproduct2subject_openaire,
            inputs="raw/openaire/researchproduct",
            outputs="ldg/openaire/researchproduct2subject"
        ),
    ], tags="openaire"
)