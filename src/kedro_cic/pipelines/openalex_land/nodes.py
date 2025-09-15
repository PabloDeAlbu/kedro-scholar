import pandas as pd
import requests
import time
from datetime import datetime, date
from pandas import json_normalize

def land_openalex_author(df: pd.DataFrame)-> pd.DataFrame:
    
    df_author = df.drop(
        columns=[
            'display_name_alternatives', 
            'summary_stats', 
            'ids', 
            'affiliations', 
            'last_known_institutions', 
            'topics', 
            'topic_share', 
            'x_concepts', 
            'counts_by_year', 
            'works_api_url'
        ]
    )

    df_author = df_author.convert_dtypes()
    df_author['load_datetime'] = pd.to_datetime(datetime.today())
    
    return df_author

def land_openalex_author_affiliation(df: pd.DataFrame)-> pd.DataFrame:

    # Selecciono columna con id de author y afiliación
    df_author = df.loc[:, ['id', 'affiliations']]
    df_author = df_author.convert_dtypes()

    # Proceso columna 'affiliations'
    df_author = df_author.explode('affiliations').reset_index(drop=True)
    affiliation_expanded = pd.json_normalize(df_author["affiliations"])
    affiliation_expanded = affiliation_expanded.loc[:,['institution.id','years']]

    df_author2affiliation = pd.concat([df_author, affiliation_expanded], axis=1)
    df_author2affiliation.drop(columns=["affiliations"], inplace=True)
    df_author2affiliation = df_author2affiliation.explode('years')

    df_author2affiliation.rename(columns={'institution.id':'institution_id'}, inplace=True)
    df_author2affiliation = df_author2affiliation.convert_dtypes()

    df_author2affiliation['load_datetime'] = pd.to_datetime(datetime.today())
    
    return df_author2affiliation

def land_openalex_author_topic(df: pd.DataFrame)-> pd.DataFrame:
    
    df_author = df.loc[:,['id', 'topics']]
    df_author = df_author.convert_dtypes() 
    
    # proceso 'topics'
    df_author2topic_exploded = df_author.explode('topics').reset_index(drop=True)
    
    df_author2topic_norm = pd.json_normalize(df_author2topic_exploded['topics'])
    df_author2topic_norm = df_author2topic_norm.loc[:,['count','id','domain.id','field.id','subfield.id']]
    df_author2topic_norm = df_author2topic_norm.rename(columns={'id':'id_topic'})

    df_author2topic = pd.concat([df_author2topic_exploded, df_author2topic_norm], axis=1)
    df_author2topic = df_author2topic.drop(columns=['topics'])

    df_author2topic.rename(columns={'domain.id':'domain_id'}, inplace=True)
    df_author2topic.rename(columns={'field.id':'field_id'}, inplace=True)
    df_author2topic.rename(columns={'subfield.id':'subfield_id'}, inplace=True)

    df_author2topic['load_datetime'] = pd.to_datetime(datetime.today())

    return df_author2topic

def land_openalex_work(df_work_raw):
    """Limpia y transforma los datos de OpenAlex para su almacenamiento en una base de datos relacional."""
    
    expected_columns = [
        'id',
        # 'doi', # doi existe en ids
        'title',
        'display_name',
        'publication_year',
        'publication_date',
        'ids',
        'language',
        'primary_location',
        'type',
        'type_crossref',
        # 'indexed_in',
        'open_access',
        # 'authorships',
        # 'institution_assertions',
        'countries_distinct_count',
        'institutions_distinct_count',
        # 'corresponding_author_ids',
        # 'corresponding_institution_ids',
        'apc_list',
        'apc_paid',
        'fwci',
        'has_fulltext',
        'fulltext_origin',
        'cited_by_count',
        'citation_normalized_percentile',
        'cited_by_percentile_year',
        'biblio',
        'is_retracted',
        'is_paratext',
        'primary_topic',
        # 'topics',
        # 'keywords',
        # 'concepts',
        # 'mesh',
        'locations_count',
        # 'locations',
        'best_oa_location',
        # 'sustainable_development_goals',
        # 'grants',
        # 'datasets',
        # 'versions',
        'referenced_works_count',
        # 'referenced_works',
        # 'related_works',
        # 'abstract_inverted_index',
        # 'abstract_inverted_index_v3',
        'cited_by_api_url',
        # 'counts_by_year',
        'updated_date',
        'created_date'
    ]

    df_work = df_work_raw.loc[:,expected_columns].reset_index(drop=True).copy()

    # Agregar columnas faltantes con NaN
    for col in expected_columns:
        if col not in df_work.columns:
            df_work[col] = pd.NA

    # ids
    df_ids = pd.json_normalize(df_work['ids']).reset_index(drop=True)
    df_work = pd.concat([df_work, df_ids], axis=1)
    df_work.drop(columns=['ids'], inplace=True)    

    # primary_location
    df_primary_location = pd.json_normalize(df_work['primary_location']).reset_index(drop=True)
    df_primary_location.rename(columns=lambda col: f'primary_location.{col}', inplace=True)
    df_primary_location.drop(columns=[
        'primary_location.source.issn',
        'primary_location.source.host_organization_lineage',
        'primary_location.source.host_organization_lineage_names'
        ],
        inplace=True)

    df_work = pd.concat([df_work, df_primary_location], axis=1)
    df_work.drop(columns=['primary_location'], inplace=True)    

    # openacess
    df_openaccess_expanded = pd.json_normalize(df_work['open_access'])
    df_work = pd.concat([df_work, df_openaccess_expanded], axis=1)
    df_work.drop(columns=['open_access'], inplace=True)    

    # apc_list
    df_apc_list = pd.json_normalize(df_work['apc_list'])
    df_apc_list.rename(columns=lambda col: f'apc_list.{col}', inplace=True)
    df_work = pd.concat([df_work, df_apc_list], axis=1)
    df_work.drop(columns=['apc_list'], inplace=True)    
 
    # apc_paid
    df_apc_paid = pd.json_normalize(df_work['apc_paid'])
    df_apc_paid.rename(columns=lambda col: f'apc_paid.{col}', inplace=True)
    df_work = pd.concat([df_work, df_apc_paid], axis=1)
    df_work.drop(columns=['apc_paid'], inplace=True)    
 
    # citation_normalized_percentile
    df_citation_normalized_percentile = pd.json_normalize(df_work['citation_normalized_percentile'])
    df_citation_normalized_percentile.rename(columns=lambda col: f'citation_normalized_percentile.{col}', inplace=True)
    df_work = pd.concat([df_work, df_citation_normalized_percentile], axis=1)
    df_work.drop(columns=['citation_normalized_percentile'], inplace=True)    

    # cited_by_percentile_year
    df_cited_by_percentile_year = pd.json_normalize(df_work['cited_by_percentile_year'])
    df_cited_by_percentile_year.rename(columns=lambda col: f'cited_by_percentile_year.{col}', inplace=True)
    df_work = pd.concat([df_work, df_cited_by_percentile_year], axis=1)
    df_work.drop(columns=['cited_by_percentile_year'], inplace=True)    

    # biblio
    df_biblio = pd.json_normalize(df_work['biblio'])
    df_biblio.rename(columns=lambda col: f'biblio.{col}', inplace=True)
    df_work = pd.concat([df_work, df_biblio], axis=1)
    df_work.drop(columns=['biblio'], inplace=True)

    # primary_topic
    df_primary_topic = pd.json_normalize(df_work['primary_topic'])
    df_primary_topic.rename(columns=lambda col: f'primary_topic.{col}', inplace=True)
    df_work = pd.concat([df_work, df_primary_topic], axis=1)
    df_work.drop(columns=['primary_topic'], inplace=True)    

    # best_oa_location
    df_best_oa_location = pd.json_normalize(df_work['best_oa_location'])
    df_best_oa_location.rename(columns=lambda col: f'best_oa_location.{col}', inplace=True)
    df_best_oa_location.drop(columns=[
        'best_oa_location.source.host_organization_lineage',
        'best_oa_location.source.host_organization_lineage_names',
        'best_oa_location.source.issn',
    ], inplace=True)
    
    df_work = pd.concat([df_work, df_best_oa_location], axis=1)
    df_work.drop(columns=['best_oa_location'], inplace=True)    

    # Agregar la fecha de carga con formato datetime
    df_work['load_datetime'] = pd.to_datetime(datetime.today())

    # Convertir tipos de datos automáticamente
    df_work = df_work.convert_dtypes()

    return df_work

def land_openalex_work_authorships(df_work_raw):

    # Seleccionar las columnas necesarias y convertir los tipos de datos
    df_work2authorships = df_work_raw[['id', 'authorships']].convert_dtypes()
    df_work2authorships.rename(columns={"id": "work_id"}, inplace=True)

    # Expandir la lista de authorships
    df_work2authorships_exploded = df_work2authorships.explode('authorships', ignore_index=True)

    # Normalizar la información de authorships
    df_authorships_norm = pd.json_normalize(df_work2authorships_exploded['authorships'])
    df_authorships_norm.rename(columns={"author.id": "author_id"}, inplace=True)
    
    # Combinar work_id con la información normalizada de authorships
    df_work2authorships = df_work2authorships_exploded[['work_id']].join(df_authorships_norm)

    # Extraer la relación work-author
    df_work2author = df_work2authorships[['work_id', 'author_id', 'author_position']]

    # Expandir la lista de instituciones asociadas a cada autor
    df_work2institution_exploded = df_work2authorships.explode('institutions', ignore_index=True)

    # Normalizar la información de instituciones
    df_institution_norm = pd.json_normalize(df_work2institution_exploded['institutions'])
    df_institution_norm.drop(columns=['lineage'], errors='ignore', inplace=True)

    # Combinar author_id con la información normalizada de instituciones
    df_author2institution = df_work2institution_exploded[['author_id']].join(df_institution_norm)

    # Combinar work_id con la información normalizada de instituciones
    df_work2institution = df_work2institution_exploded[['work_id']].join(df_institution_norm)
    
    df_work2author['load_datetime'] = date.today()
    df_work2institution['load_datetime'] = date.today()
    df_author2institution['load_datetime'] = date.today()

    return df_work2author, df_work2institution, df_author2institution

def land_openalex_work_concept(df_work_raw):
    df_work = df_work_raw.loc[:,['id','concepts']]
    df_work = df_work.convert_dtypes()

    df_work2concepts_exploded = df_work.explode('concepts').reset_index(drop=True)
    df_work2concepts_norm = pd.json_normalize(df_work2concepts_exploded['concepts'])
    df_work2concepts_norm.rename(columns={'id':'concept_id'}, inplace=True)

    df_work = df_work2concepts_exploded.loc[:,'id']
    df_work2concepts = pd.concat((df_work, df_work2concepts_norm), axis=1)
    
    df_work2concepts['load_datetime'] = pd.to_datetime(datetime.today())

    return df_work2concepts

def land_openalex_work_corresponding_author_ids(df_work_raw):
    df_work = df_work_raw.loc[:,['id','corresponding_author_ids']]
    df_work = df_work.convert_dtypes()

    df_work2corresponding_author_ids = df_work.explode('corresponding_author_ids')

    df_work2corresponding_author_ids['load_datetime'] = pd.to_datetime(datetime.today())

    return df_work2corresponding_author_ids

def land_openalex_work_referenced_works(df_work_raw):
    df_work = df_work_raw.loc[:,['id','referenced_works']]
    df_work = df_work.convert_dtypes()
    df_work2referenced_works_exploded =  df_work.explode('referenced_works')
    df_work2referenced_works = df_work2referenced_works_exploded.reset_index(drop=True)

    df_work2referenced_works['load_datetime'] = pd.to_datetime(datetime.today())

    return df_work2referenced_works


def land_openalex_work_topics(df_work_raw):
    df_work = df_work_raw.loc[:,['id','topics']]
    df_work = df_work.convert_dtypes()
    
    # Proceso topics
    df_work2topics_exploded = df_work.explode('topics')
    df_work2topics_norm = pd.json_normalize(df_work2topics_exploded['topics'])
    df_work2topics_exploded = df_work2topics_exploded.reset_index(drop=True)
    df_work2topics_norm.rename(columns={'id':'topic_id'}, inplace=True)
   
    # Creación de df con work y sus topics
    df_work2topics = pd.concat((df_work2topics_exploded['id'], df_work2topics_norm), axis=1)

    df_work2topics['load_datetime'] = pd.to_datetime(datetime.today())

    return df_work2topics

def land_openalex_work_location(df_work_raw):

    df_work_location = df_work_raw.explode('locations').reset_index(drop=True)

    df_work_location = pd.concat([df_work_location['id'], json_normalize(df_work_location['locations'])], axis=1)

    df_work_location.columns = df_work_location.columns.str.replace('.', '_')

    df_work_location = df_work_location[[
        'id', 
        'source_id', 'source_display_name', 'source_is_core', 'source_type',
        'source_host_organization', 'source_host_organization_name',
        'is_accepted', 'is_oa', 'is_published', 'landing_page_url',
        'license', 'license_id', 'pdf_url', 'version',
        # 'source_host_organization_lineage', 'source_host_organization_lineage_names', 'source_issn',
        'source_is_in_doaj', 'source_is_oa', 'source_issn_l'
    ]]

    df_work_location['load_datetime'] = pd.to_datetime(datetime.today())

    return df_work_location
