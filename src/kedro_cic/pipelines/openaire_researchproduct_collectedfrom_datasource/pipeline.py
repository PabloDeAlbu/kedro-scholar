"""
This is a boilerplate pipeline 'openaire_researchproduct_relOrganizationId'
generated using Kedro 0.19.9
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    fetch_openaire_graph_researchproduct,
    land_openaire_graph_researchproduct,
)



def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="fetch_openaire_graph_researchproduct",
            func=fetch_openaire_graph_researchproduct,
            inputs=[
                "params:openaire_researchproduct_collectedfrom_datasource_fetch_options.filter",
                "params:openaire_researchproduct_collectedfrom_datasource_fetch_options.filter_value",
                "params:openaire_researchproduct_collectedfrom_datasource_fetch_options.access_token",
                "params:openaire_researchproduct_collectedfrom_datasource_fetch_options.refresh_token",
                "params:fetch_options.env",
            ],
            outputs=[
                "raw/openaire_graph/researchproduct#parquet",
                "raw/openaire_graph/researchproduct_dev#parquet",
                ],
        ),
        node(
            name="land_openaire_graph_researchproduct",
            func=land_openaire_graph_researchproduct,
            inputs="raw/openaire_graph/researchproduct#parquet",
            outputs=[
                "ldg/openaire_graph/researchproduct",
                "ldg/openaire_graph/researchproduct2originalId",
                "ldg/openaire_graph/researchproduct2author",
                "ldg/openaire_graph/researchproduct2subject",
                "ldg/openaire_graph/researchproduct2pid",
                "ldg/openaire_graph/researchproduct2url"
                ]
        ),
    ], tags="openaire_graph_researchproduct")
