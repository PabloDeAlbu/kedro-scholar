import pandas as pd

def get_fact_publication(hub_openalex_work, sat_openalex_work, sal_openalex_work, hub_openalex_doi, link_openaire_researchproduct_doi, sat_openaire_researchproduct):
    fact_publication_openalex = pd.merge(
        hub_openalex_work,
        sat_openalex_work
        ).drop(columns=['load_datetime','source', 'hashdiff'])

    fact_publication = pd.merge(
        fact_publication_openalex[['work_hk','title','type','publication_year','cited_by_count','oa_status']],
        sal_openalex_work[['work_hk','doi_hk']]
    )

    fact_publication = pd.merge(
        fact_publication,
        hub_openalex_doi[['doi_hk','doi']],
        how='left'
    )

    fact_publication = pd.merge(
        fact_publication,
        link_openaire_researchproduct_doi,
        on="doi_hk",
        how='left'
    )

    fact_publication = pd.merge(
        fact_publication,
        sat_openaire_researchproduct[['researchproduct_hk','citation_count','citation_class','impulse','impulse_class','influence','influence_class','popularity','popularity_class','downloads','views']],
        on="researchproduct_hk",
        how='left'
    )

    fact_publication.rename(columns={'citation_count': 'citation_count_openaire'}, inplace=True)
    fact_columns = ['title','publication_year','type','cited_by_count','oa_status','citation_count_openaire','downloads','views','citation_class','impulse','impulse_class','influence','influence_class','popularity','popularity_class']
    fact_publication = fact_publication[fact_columns].sort_values('cited_by_count', ascending=False)
    return fact_publication
