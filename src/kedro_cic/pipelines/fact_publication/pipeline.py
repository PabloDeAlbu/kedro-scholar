from kedro.pipeline import Pipeline, node, pipeline
from kedro.pipeline import Pipeline, pipeline
from .nodes import get_fact_publication, get_fact_publication_openaire, get_fact_publication_openalex


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="get_fact_publication_openaire",
            func=get_fact_publication_openaire,
            inputs=[
                'stg_coar/hub_coar_resourcetype',
                'stg_openaire/hub_openaire_doi',
                'stg_openaire/hub_openaire_researchproduct',
                'stg_coar/link_coar_openaire_resourcetype',
                'stg_openaire/link_openaire_researchproduct_type',
                'stg_openaire/link_openaire_researchproduct_doi',
                'stg_coar/sat_coar_resourcetype',
                'stg_openaire/sat_openaire_researchproduct',

            ],
            outputs="dm/fact_publication_openaire"
        ),
        node(
            name="get_fact_publication_openalex",
            func=get_fact_publication_openalex,
            inputs=[
                'stg_coar/hub_coar_resourcetype',
                'stg_openalex/hub_openalex_work',
                'stg_openalex/hub_openalex_work_type',
                'stg_openalex/hub_openalex_doi',
                'stg_coar/link_coar_openalex_resourcetype',
                'stg_openalex/link_openalex_work_type',
                'stg_openalex/sal_openalex_work',
                'stg_coar/sat_coar_resourcetype',
                'stg_openalex/sat_openalex_work',
            ],
            outputs="dm/fact_publication_openalex"
        ),
        node(
            name="get_fact_publication",
            func=get_fact_publication,
            inputs=[
                "dm/fact_publication_openaire", 
                "dm/fact_publication_openalex", 
            ],
            outputs="dm/fact_publication"
        ),
        
    ], tags="fact_publication")
