import pandas as pd
import requests
from datetime import date
from pandas import json_normalize

def fetch_author_openalex(institution_ror, env):
    cursor = '*'
    base_url = 'https://api.openalex.org/authors?filter=affiliations.institution.ror:{}&cursor={}'
    iteration_limit = 5
    iteration_count = 0

    url = base_url.format(institution_ror, cursor)
    api_response = requests.get(url).json()

    print(f'Iteration count: {iteration_count}')
    print(f'GET {url}')

    # creo dataframe con las columnas del primer resultado 
    df = pd.DataFrame.from_dict(api_response['results'])

    # update cursor
    cursor = api_response['meta']['next_cursor']
    url = base_url.format(institution_ror, cursor)

    while cursor:

        if env == 'dev' and iteration_count >= iteration_limit:
            break

        # request api with updated cursor
        url = base_url.format(institution_ror, cursor)

        iteration_count += 1
        print(f'Iteration count: {iteration_count}')
        print(f'GET {url}')

        api_response = requests.get(url).json()

        df_tmp = pd.DataFrame.from_dict(api_response['results'])

        df = pd.concat([df, df_tmp])

        # update cursor
        cursor = api_response['meta']['next_cursor']

    return df

def fetch_work_openalex(institution_ror, env):
    cursor = '*'
    base_url = 'https://api.openalex.org/works?filter=institutions.ror:{}&cursor={}'
    iteration_limit = 5
    iteration_count = 0

    url = base_url.format(institution_ror, cursor)
    api_response = requests.get(url).json()

    print(f'Iteration count: {iteration_count}')
    print(f'GET {url}')

    # creo dataframe con las columnas del primer resultado 
    df = pd.DataFrame.from_dict(api_response['results'])

    # update cursor
    cursor = api_response['meta']['next_cursor']
    url = base_url.format(institution_ror, cursor)

    while cursor:

        if env == 'dev' and iteration_count >= iteration_limit:
            break

        # request api with updated cursor
        url = base_url.format(institution_ror, cursor)

        iteration_count += 1
        print(f'Iteration count: {iteration_count}')
        print(f'GET {url}')

        api_response = requests.get(url).json()

        df_tmp = pd.DataFrame.from_dict(api_response['results'])

        df = pd.concat([df, df_tmp])

        # update cursor
        cursor = api_response['meta']['next_cursor']

    return df, df.head(1000)

def fetch_work_dimensions_openalex():
    base_url = 'https://api.openalex.org/works?group_by={}'

    group_by_attribute = 'type'
    url = base_url.format(group_by_attribute)
    api_response = requests.get(url).json()
    print(f'GET {url}')
    df_worktype = pd.DataFrame.from_dict(api_response['group_by'])

    group_by_attribute = 'language'
    url = base_url.format(group_by_attribute)
    api_response = requests.get(url).json()
    print(f'GET {url}')
    df_language = pd.DataFrame.from_dict(api_response['group_by'])

    group_by_attribute = 'primary_location.license'
    url = base_url.format(group_by_attribute)
    api_response = requests.get(url).json()
    print(f'GET {url}')
    df_license = pd.DataFrame.from_dict(api_response['group_by'])

    return df_worktype, df_language, df_license

def land_author_openalex(df: pd.DataFrame)-> pd.DataFrame:
    
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
    df_author['load_datetime'] = date.today()
    
    return df_author

def land_author2affiliation_openalex(df: pd.DataFrame)-> pd.DataFrame:

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

    df_author2affiliation['load_datetime'] = date.today()
    
    return df_author2affiliation

def land_author2topic_openalex(df: pd.DataFrame)-> pd.DataFrame:
    
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

    df_author2topic['load_datetime'] = date.today()
    return df_author2topic


def land_work_dimensions_openalex(df_worktype, df_language, df_license):
    df_worktype['load_datetime'] = date.today()
    df_language['load_datetime'] = date.today()
    df_license['load_datetime'] = date.today()

    df_worktype.rename(columns={'key':'worktype_id','key_display_name':'worktype_display_name'}, inplace=True)
    df_language.rename(columns={'key':'language_id','key_display_name':'language_display_name'}, inplace=True)
    df_license.rename(columns={'key':'license_id','key_display_name':'license_display_name'}, inplace=True)

    return df_worktype, df_language, df_license

def land_work_openalex(df_work_raw):
    df_work = df_work_raw.loc[:,
        [
            'id',
            'doi',
            'title',
            'display_name',
            'publication_year',
            'publication_date',
            'language',
            'type',
            'type_crossref',
            'countries_distinct_count',
            'institutions_distinct_count',
            'fwci',
            'has_fulltext',
            'fulltext_origin',
            'cited_by_count',
            'is_retracted',
            'is_paratext',
            'locations_count',
            'referenced_works_count',
            'cited_by_api_url',
            'updated_date',
            'created_date'
        ]
    ]

    df_work = df_work.convert_dtypes()
    df_work.rename(columns={'id':'work_id'}, inplace=True)
    
    return df_work

def land_work2apc_list_openalex(df_work_raw):
    df_work = df_work_raw.loc[:,['id','apc_list']]
    df_work = df_work.convert_dtypes()

    df_work2apc_list = pd.json_normalize(df_work['apc_list'])
    df_work2apc_list = pd.concat((df_work_raw.loc[:,'id'].reset_index(drop=True),df_work2apc_list), axis=1)
    
    # Rename de columnas
    df_work2apc_list.rename(columns={'id': 'work_id'}, inplace=True)

    return df_work2apc_list

def land_work2apc_paid_openalex(df_work_raw):
    df_work = df_work_raw.loc[:,['id','apc_paid']]
    df_work = df_work.convert_dtypes()

    df_work2apc_paid = pd.json_normalize(df_work['apc_paid'])
    df_work2apc_paid = pd.concat((df_work_raw.loc[:,'id'].reset_index(drop=True),df_work2apc_paid), axis=1)

    # Rename de columnas
    df_work2apc_paid.rename(columns={'id': 'work_id'}, inplace=True)

    return df_work2apc_paid

def land_work2authorships_openalex(df_work_raw):
    df_work2authorships = df_work_raw.loc[:,['id','authorships']]
    df_work2authorships = df_work2authorships.convert_dtypes()
    df_work2authorships.rename(columns={"id":'work_id'}, inplace=True)

    df_work2authorships_exploded = df_work2authorships.explode('authorships').reset_index(drop=True)
    df_work2authorships_norm =  pd.json_normalize(df_work2authorships_exploded['authorships'])
    df_work2authorships_norm.rename(columns={"author.id":'author_id'}, inplace=True)
    
    df_work = df_work2authorships_exploded.loc[:,'work_id']
    df_work2authorships = pd.concat((df_work, df_work2authorships_norm), axis=1)

    df_work2author = df_work2authorships.loc[:,['work_id','author_id','author_position']]
    df_author2institution_exploded = df_work2authorships.loc[:,['author_id','institutions']].explode('institutions').reset_index(drop=True)

    df_author = df_author2institution_exploded.loc[:,'author_id']
    df_institution_norm = pd.json_normalize(df_author2institution_exploded['institutions'])
    df_institution_norm.drop(columns=['lineage'], inplace=True)
    df_author2institution = pd.concat((df_author, df_institution_norm), axis=1)

    return df_work2author, df_author2institution

def land_work2ids_openalex(df_work_raw):
    df_work = df_work_raw.loc[:,['id','ids']]
    df_work = df_work.convert_dtypes()

    df_work2ids = pd.json_normalize(df_work['ids'])
    return df_work2ids

def land_work2concepts_openalex(df_work_raw):
    df_work = df_work_raw.loc[:,['id','concepts']]
    df_work = df_work.convert_dtypes()

    df_work2concepts_exploded = df_work.explode('concepts').reset_index(drop=True)
    df_work2concepts_norm = pd.json_normalize(df_work2concepts_exploded['concepts'])
    df_work2concepts_norm.rename(columns={'id':'concept_id'}, inplace=True)

    df_work = df_work2concepts_exploded.loc[:,'id']
    df_work2concepts = pd.concat((df_work, df_work2concepts_norm), axis=1)
    
    # Rename de columna id
    df_work2concepts.rename(columns={'id':'work_id'}, inplace=True)
    return df_work2concepts

def land_work2corresponding_author_ids_openalex(df_work_raw):
    df_work = df_work_raw.loc[:,['id','corresponding_author_ids']]
    df_work = df_work.convert_dtypes()

    df_work2corresponding_author_ids_exploded = df_work.explode('corresponding_author_ids')
    df_work2corresponding_author_ids = df_work2corresponding_author_ids_exploded.rename(columns={'id':'work_id'})

    return df_work2corresponding_author_ids

def land_work2primary_topic_openalex(df_work_raw):
    df_work = df_work_raw.loc[:,['id','primary_topic']]
    df_work = df_work.convert_dtypes()
    df_work2primary_topic_norm = pd.json_normalize(df_work['primary_topic']).loc[:,['id','domain.id','field.id','subfield.id']]

    # 
    df_work2primary_topic_norm.rename(columns={'id':'topic.id'}, inplace=True)
    df_work2primary_topic = pd.concat((df_work['id'].reset_index(drop=True),df_work2primary_topic_norm), axis=1)

    # Rename de columnas
    df_work2primary_topic.rename(columns=lambda col: col.replace('.', '_'), inplace=True)
    df_work2primary_topic.rename(columns={'id': 'work_id'}, inplace=True)


    return df_work2primary_topic

def land_work2primary_location_openalex(df_work_raw):
    df_work = df_work_raw.loc[:,['id','primary_location']]
    df_work = df_work.convert_dtypes()

    df_work2primarylocation = pd.json_normalize(df_work['primary_location'])
    df_work2primarylocation.drop(columns=
        [
            'source.host_organization_lineage',
            'source.host_organization_lineage_names',
            'source.issn'
        ],
        inplace=True
    )

    df_work = df_work['id'].reset_index(drop=True)
    df_work2primarylocation = pd.concat((df_work, df_work2primarylocation), axis=1)

    df_work2primarylocation.rename(columns={'id':'work_id'}, inplace=True)

    return df_work2primarylocation

def land_work2referenced_works_openalex(df_work_raw):
    df_work = df_work_raw.loc[:,['id','referenced_works']]
    df_work = df_work.convert_dtypes()
    df_work2referenced_works_exploded =  df_work.explode('referenced_works')
    df_work2referenced_works = df_work2referenced_works_exploded.reset_index(drop=True)

    # Reemplazar '.' por '_' en los nombres de las columnas
    df_work2referenced_works.rename(columns={'id': 'work_id'})

    return df_work2referenced_works


def land_work2topics_openalex(df_work_raw):
    df_work = df_work_raw.loc[:,['id','topics']]
    df_work = df_work.convert_dtypes()
    
    # Proceso topics
    df_work2topics_exploded = df_work.explode('topics')
    df_work2topics_norm = pd.json_normalize(df_work2topics_exploded['topics'])
    df_work2topics_exploded = df_work2topics_exploded.reset_index(drop=True)
    df_work2topics_norm.rename(columns={'id':'topic_id'}, inplace=True)
   
    # Creación de df con work y sus topics
    df_work2topics = pd.concat((df_work2topics_exploded['id'], df_work2topics_norm), axis=1)

    # Rename de columnas
    df_work2topics.rename(columns=lambda col: col.replace('.', '_'), inplace=True)
    df_work2topics.rename(columns={'id': 'work_id'}, inplace=True)

    return df_work2topics

def land_work2location_openalex(df_work_raw):

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

    df_work_location['load_datetime'] = date.today()
    df_work_location.rename(columns={'id':'work_id'}, inplace=True)

    return df_work_location
