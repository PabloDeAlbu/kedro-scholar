"""
This is a boilerplate pipeline 'enrich_ir_metadata_from_openalex_openaire'
generated using Kedro 0.19.9
"""

from kedro.pipeline import Pipeline, pipeline
from .nodes import enrich_ir_metadata_from_openalex_openaire

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="enrich_ir_metadata_from_openalex_openaire",
            func=enrich_ir_metadata_from_openalex_openaire,
            inputs=[
                'stg_openaire/dim_doi_openaire',
                'stg_openalex/dim_author_openalex',
                'stg_openalex/dim_institution_openalex',
                'stg_openaire/bridge_publication_oaiidentifier_openaire',
                'stg_openaire/bridge_publication_doi_openaire',
                'stg_openalex/bridge_author_institution_openalex',
                'stg_openalex/fact_publication_openalex',
                'stg_openaire/fact_publication_openaire',
                'stg_dspace5/fact_publication_dspace5',
            ],
            outputs=[
                "dm/fact_publication_ir_openaire ",
                "dm/publication_to_import_openalex",
                "dm/institutional_authors_openalex",
            ]
        )
    ], tags="enrich_ir")
