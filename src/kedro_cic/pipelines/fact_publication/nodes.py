import pandas as pd

def get_fact_publication_openaire(
        hub_coar_resourcetype,
        hub_openaire_doi,
        hub_openaire_researchproduct,
        link_coar_openaire_resourcetype,
        link_openaire_researchproduct_type,
        link_openaire_researchproduct_doi,
        sat_coar_resourcetype,
        sat_openaire_researchproduct,
    ):

    hub_coar_resourcetype.drop(columns=['load_datetime', 'source'], inplace=True)
    hub_openaire_doi.drop(columns=['load_datetime', 'source'], inplace=True)
    hub_openaire_researchproduct.drop(columns=['load_datetime', 'source'], inplace=True)
    link_coar_openaire_resourcetype.drop(columns=['load_datetime', 'source'], inplace=True)
    link_openaire_researchproduct_type.drop(columns=['load_datetime', 'source'], inplace=True)
    link_openaire_researchproduct_doi.drop(columns=['load_datetime', 'source'], inplace=True)
    sat_coar_resourcetype.drop(columns=['load_datetime', 'source', 'hashdiff'], inplace=True)
    sat_openaire_researchproduct.drop(columns=['load_datetime', 'source', 'hashdiff'], inplace=True)

    fact_publication_openaire = pd.merge(
        hub_openaire_researchproduct,
        link_openaire_researchproduct_type
    )

    fact_publication_openaire = pd.merge(
        fact_publication_openaire,
        link_openaire_researchproduct_doi
    )

    fact_publication_openaire = pd.merge(
        fact_publication_openaire,
        hub_openaire_doi
    )

    fact_publication_openaire = pd.merge(
        fact_publication_openaire,
        link_coar_openaire_resourcetype
    ).drop(columns=['type_hk', 'researchproduct_type_hk', 'link_coar_openaire_hk'])

    fact_publication_openaire = pd.merge(
        fact_publication_openaire,
        hub_coar_resourcetype
    )

    fact_publication_openaire = pd.merge(
        fact_publication_openaire,
        sat_coar_resourcetype
    ).drop(columns=['coar_hk'])

    fact_publication_openaire = pd.merge(
        fact_publication_openaire,
        sat_openaire_researchproduct
    )

    return fact_publication_openaire

def get_fact_publication_openalex(
        hub_coar_resourcetype,
        hub_openalex_work,
        hub_openalex_work_type,
        hub_openalex_doi,
        link_coar_openalex_resourcetype,
        link_openalex_work_type,
        sal_openalex_work,
        sat_coar_resourcetype,
        sat_openalex_work,
    ):

    # hubs
    hub_coar_resourcetype.drop(columns=['load_datetime','source'], inplace=True)
    hub_openalex_work.drop(columns=['load_datetime','source'], inplace=True)
    hub_openalex_work_type.drop(columns=['load_datetime','source'], inplace=True)
    hub_openalex_doi.drop(columns=['load_datetime','source'], inplace=True)
    
    #links
    link_coar_openalex_resourcetype.drop(columns=['load_datetime','source'], inplace=True)
    link_openalex_work_type.drop(columns=['load_datetime','source'], inplace=True)
    sal_openalex_work.drop(columns=['load_datetime','source'], inplace=True)

    # satellites
    sat_coar_resourcetype.drop(columns=['load_datetime','source','hashdiff'], inplace=True)
    sat_openalex_work.drop(columns=['load_datetime','source','hashdiff'], inplace=True)

    fact_publication_openalex = pd.merge(
        hub_openalex_work,
        link_openalex_work_type
    )

    fact_publication_openalex = pd.merge(
        fact_publication_openalex,
        sal_openalex_work
    )

    fact_publication_openalex = pd.merge(
        fact_publication_openalex,
        hub_openalex_doi
    )

    fact_publication_openalex = pd.merge(
        fact_publication_openalex,
        link_coar_openalex_resourcetype
    ).drop(columns=['type_hk', 'work_type_hk', 'link_coar_openalex_hk'])

    fact_publication_openalex = pd.merge(
        fact_publication_openalex,
        hub_coar_resourcetype
    )

    fact_publication_openalex = pd.merge(
        fact_publication_openalex,
        sat_coar_resourcetype
    ).drop(columns=['coar_hk'])

    fact_publication_openalex = pd.merge(
        fact_publication_openalex,
        sat_openalex_work
    )

    return fact_publication_openalex

def get_fact_publication(
        fact_publication_openaire,
        fact_publication_openalex,
    ):

    # Agregar columnas de presencia en cada fuente
    fact_publication_openaire['in_openaire'] = 1
    fact_publication_openalex['in_openalex'] = 1

    fact_publication = pd.merge(
        fact_publication_openaire,
        fact_publication_openalex,
        on='doi_hk',
        how='outer',
        suffixes=('_delete','')
    )

    # Rellenar NaN en in_openaire con 0
    fact_publication['in_openaire'] = fact_publication['in_openaire'].fillna(0).astype(int)
    fact_publication['cited_by_count'] = fact_publication['cited_by_count'].fillna(0).astype(int)

    # Eliminar columnas terminadas en '_delete'
    fact_publication = fact_publication.loc[:, ~fact_publication.columns.str.endswith('_delete')]
    fact_publication = fact_publication.loc[:, ~fact_publication.columns.str.endswith('_hk')]

    # Ordenar por cited_by_count en orden descendente
    fact_publication = fact_publication.sort_values(by='cited_by_count', ascending=False)

    return fact_publication
