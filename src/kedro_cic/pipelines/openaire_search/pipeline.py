from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    fetch_openaire_search_researchproduct,
    land_openaire_search_researchproduct,
    land_openaire_search_researchproduct2creator,
    land_openaire_search_researchproduct2measure,
    land_openaire_search_researchproduct2pid_openaire,
    land_openaire_search_researchproduct2relevantdate,
    land_openaire_search_researchproduct2subject,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="fetch_openaire_search_researchproduct",
            func=fetch_openaire_search_researchproduct,
            inputs=[
                "stg/dim_doi",
                #"stg_openaire_graph/hub_openaire_graph_originalid",
                "params:openaire_fetch_options.access_token",
                "params:fetch_options.env",
            ],
            outputs="raw/openaire_search/researchproduct",
        ),
       node(
            name="land_openaire_search_researchproduct",
            func=land_openaire_search_researchproduct,
            inputs="raw/openaire_search/researchproduct",
            outputs="ldg/openaire_search/researchproduct"
        ),
        node(
            name="land_openaire_search_researchproduct2creator",
            func=land_openaire_search_researchproduct2creator,
            inputs="raw/openaire_search/researchproduct",
            outputs="ldg/openaire_search/researchproduct2creator"
        ),
        node(
            name="land_openaire_search_researchproduct2measure",
            func=land_openaire_search_researchproduct2measure,
            inputs="raw/openaire_search/researchproduct",
            outputs="ldg/openaire_search/researchproduct2measure"
        ),
        node(
            name="land_openaire_search_researchproduct2pid_openaire",
            func=land_openaire_search_researchproduct2pid_openaire,
            inputs="raw/openaire_search/researchproduct",
            outputs="ldg/openaire_search/researchproduct2pid"
        ),
        node(
            name="land_openaire_search_researchproduct2relevantdate",
            func=land_openaire_search_researchproduct2relevantdate,
            inputs="raw/openaire_search/researchproduct",
            outputs="ldg/openaire_search/researchproduct2relevantdate"
        ),
        node(
            name="land_openaire_search_researchproduct2subject",
            func=land_openaire_search_researchproduct2subject,
            inputs="raw/openaire_search/researchproduct",
            outputs="ldg/openaire_search/researchproduct2subject"
        ),
    ], tags="openaire_search"
)