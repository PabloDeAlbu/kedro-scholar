from kedro.pipeline import Pipeline, node, pipeline
from kedro.pipeline import Pipeline, pipeline
from .nodes import get_fact_publication


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="fact_publication",
            func=get_fact_publication,
            inputs=[
                "stg_openalex/hub_openalex_work", 
                "stg_openalex/sat_openalex_work", 
                "stg_openalex/sal_openalex_work", 
                "stg_openalex/hub_openalex_doi", 
                "stg_openaire/link_openaire_researchproduct_doi", 
                "stg_openaire/sat_openaire_researchproduct"
            ],
            outputs="stg/fact_publication"
        ),
        
    ])
