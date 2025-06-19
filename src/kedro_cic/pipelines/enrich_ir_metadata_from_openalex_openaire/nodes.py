import pandas as pd

def enrich_ir_metadata_from_openalex_openaire(dim_doi_openaire, dim_author_openalex, dim_institution_openalex, bridge_publication_oaiidentifier_openaire, bridge_publication_doi_openaire, bridge_author_institution_openalex, fact_publication_openalex,fact_publication_openaire,fact_publication_dspace5):
    sedici_filter = bridge_publication_oaiidentifier_openaire["original_id"].str.contains("oai:sedici.unlp.edu.ar:10915", na=False)

    dim_oai_id_openaire = bridge_publication_oaiidentifier_openaire[sedici_filter].copy()

    dim_oai_id_openaire["handle"] = dim_oai_id_openaire["original_id"].str.extract(r"(10915/\d+)")

    dim_oai_id_openaire[['researchproduct_id','original_id','handle']]

    publication_ir_openaire = pd.merge(
        dim_oai_id_openaire,
        fact_publication_dspace5,
        on='handle'
    )

    fact_publication_ir_openaire = pd.merge(
        fact_publication_openaire,
        publication_ir_openaire,
        on='researchproduct_id'
    )

    publication_ir_openaire = pd.merge(
        publication_ir_openaire,
        bridge_publication_doi_openaire,
    )

    publication_ir_openaire = pd.merge(
        publication_ir_openaire,
        dim_doi_openaire
    )

    fact_publication = pd.merge(
        publication_ir_openaire,
        fact_publication_openalex,
        on='doi',
        how='outer'
    )

    not_in_ir = fact_publication['researchproduct_id'].isna()
    publication_to_import = fact_publication[not_in_ir]
    publication_to_import.drop(columns=['researchproduct_id','researchproduct_hk','handle','doi_hk','doi'], inplace=True)

    # autores de las publicaciones a importar
    ror_filter = dim_institution_openalex['ror'] == 'https://ror.org/01tjs6929'
    institution_author_openalex = pd.merge(
        bridge_author_institution_openalex,
        dim_institution_openalex[ror_filter]
    )

    institutional_authors = pd.merge(
        institution_author_openalex,
        dim_author_openalex,
        on='author_hk'
    ).sort_values(by=['works_count', 'cited_by_count'], ascending=False)

    institutional_authors.drop(columns=['author_institution_hk','author_hk','institution_hk'], inplace=True)

    institutional_authors = institutional_authors[['author_id','display_name_y','works_count','cited_by_count']]

    return fact_publication_ir_openaire, publication_to_import, institutional_authors