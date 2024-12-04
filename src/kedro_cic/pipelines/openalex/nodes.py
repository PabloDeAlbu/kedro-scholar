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
    df_work
    return df_work

def land_work2authorship_openalex(df_work_raw):

    df_work = df_work_raw.convert_dtypes()
    df_work = df_work.loc[:,['id','authorships']]

    df_work_authorship = df_work.explode('authorships').reset_index(drop=True)

    # Normalizar el JSON de la columna 'authorships' y mantener la asociación con 'id'
    df_work_authorship = pd.concat([df_work_authorship['id'], json_normalize(df_work_authorship['authorships'])], axis=1)

    # remove affiliations.
    # https://docs.openalex.org/api-entities/works/work-object/authorship-object#affiliations
    #   "This information will be redundant with institutions below, but is useful if you need to know about what we used to match institutions."
    df_work_authorship.drop(columns=['affiliations'], inplace=True)

    df_work_authorship = df_work_authorship.explode('institutions').reset_index(drop=True)
    df_institution = json_normalize(df_work_authorship['institutions'])
    df_institution = df_institution.add_prefix('institution_')
    df_work_authorship = pd.concat((df_work_authorship.drop(columns=['institutions']), df_institution), axis=1)

    df_work_authorship.columns = df_work_authorship.columns.str.replace('.', '_')

    df_work_authorship = df_work_authorship[['id','author_id','author_display_name','author_orcid','author_position','is_corresponding','institution_id','institution_display_name','institution_ror','institution_type','institution_country_code']]

    df_work_authorship.rename(columns={'id':'work_id'}, inplace=True)

    df_work_authorship['load_datetime'] = date.today()

    return df_work_authorship

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
