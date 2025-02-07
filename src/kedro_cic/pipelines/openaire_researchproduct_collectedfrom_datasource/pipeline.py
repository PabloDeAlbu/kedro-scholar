"""
This is a boilerplate pipeline 'openaire_researchproduct_collectedfrom_datasource'
generated using Kedro 0.19.9
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    fetch_openaire_researchproduct_collectedfrom_datasource,
    land_openaire_researchproduct_collectedfrom_datasource,
)



def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="fetch_openaire_researchproduct_collectedfrom_datasource",
            func=fetch_openaire_researchproduct_collectedfrom_datasource,
            inputs=[
                "params:openaire_researchproduct_collectedfrom_datasource_fetch_options.relCollectedFromDatasourceId",
                "params:openaire_researchproduct_collectedfrom_datasource_fetch_options.access_token",
                "params:openaire_researchproduct_collectedfrom_datasource_fetch_options.refresh_token",
                "params:fetch_options.env",
            ],
            outputs=[
                "raw/openaire/researchproduct_collectedfrom_datasource#parquet",
                "raw/openaire/researchproduct_collectedfrom_datasource_dev#parquet",
                ],
        ),
        node(
            name="land_openaire_researchproduct_collectedfrom_datasource",
            func=land_openaire_researchproduct_collectedfrom_datasource,
            inputs="raw/openaire/researchproduct_collectedfrom_datasource#parquet",
            outputs=[
                "ldg/openaire_graph/researchproduct",
                "ldg/openaire_graph/researchproduct2originalId",
                "ldg/openaire_graph/researchproduct2author",
                "ldg/openaire_graph/researchproduct2subject",
                "ldg/openaire_graph/researchproduct2pid",
                "ldg/openaire_graph/researchproduct2url"
                ]
        ),
    ], tags="openaire_researchproduct_collectedfrom_datasource")
