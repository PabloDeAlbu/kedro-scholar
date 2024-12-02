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


    return df

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

def land_work_dimensions_openalex(df_worktype, df_language, df_license):
    df_worktype['load_datetime'] = date.today()
    df_language['load_datetime'] = date.today()
    df_license['load_datetime'] = date.today()

    df_worktype.rename(columns={'key':'worktype_id','key_display_name':'worktype_display_name'}, inplace=True)
    df_language.rename(columns={'key':'language_id','key_display_name':'language_display_name'}, inplace=True)
    df_license.rename(columns={'key':'license_id','key_display_name':'license_display_name'}, inplace=True)

    return df_worktype, df_language, df_license

def land_work_openalex(df_work_raw):
    return df_work_raw
    df_work_raw = df_work_raw.convert_dtypes()
    df_work = df_work_raw.select_dtypes(exclude=['object'])
    df_work.reset_index(drop=True, inplace=True)

    # flat "ids"
    df_ids = json_normalize(df_work_raw['ids'])
    df_ids.drop(columns=['doi','openalex'], inplace=True)
    df_ids = df_ids.convert_dtypes()
    df_ids.fillna('NO DATA', inplace=True)

    df_work = pd.concat([df_work,df_ids], axis=1)
    df_work['doi'] = '10.' + df_work['doi'].str.extract(r'10\.(.*)')[0].str.lower()

    # flat "biblio"
    df_biblio = json_normalize(df_work_raw['biblio'])
    df_biblio = df_biblio.add_prefix('biblio_')
    df_biblio = df_biblio.convert_dtypes()
    df_biblio.fillna('NO DATA', inplace=True)

    df_work = df_work.join(df_biblio)

    # flat "open_access"
    df_open_access = json_normalize(df_work_raw['open_access'])
    df_open_access = df_open_access.convert_dtypes()
    df_open_access.fillna('NO DATA', inplace=True)
    df_work = df_work.join(df_open_access)

    # flat "cited_by_percentile_year"
    df_cited_by_percentile_year = json_normalize(df_work_raw['cited_by_percentile_year'])

    #FutureWarning: A value is trying to be set on a copy of a DataFrame or Series through chained     warnings.py:109
    # assignment using an inplace method.
    # The behavior will change in pandas 3.0. This inplace method will never work because the intermediate object on which we are setting values always behaves as a
    # copy.
    # For example, when doing 'df[col].method(value, inplace=True)', try using 'df.method({col: value}, inplace=True)' or df[col] = df[col].method(value) instead, to
    # perform the operation inplace on the original object.

    df_cited_by_percentile_year['max'].fillna(0, inplace=True)
    df_cited_by_percentile_year['min'].fillna(0, inplace=True)
    df_cited_by_percentile_year = df_cited_by_percentile_year.add_prefix('cited_by_percentile_year_')
    df_cited_by_percentile_year.fillna('NO DATA', inplace=True)
    df_cited_by_percentile_year = df_cited_by_percentile_year.convert_dtypes()
    df_work = df_work.join(df_cited_by_percentile_year)

    # citation_normalized_percentile
    df_cnp = json_normalize(df_work_raw['citation_normalized_percentile'])
    # fillna citation_normalized_percentile
    df_cnp['is_in_top_10_percent'].fillna('NO DATA', inplace=True)
    df_cnp['is_in_top_1_percent'].fillna('NO DATA', inplace=True)
    df_cnp['value'].fillna(0, inplace=True)
    # add prefix
    df_cnp = df_cnp.add_prefix('citation_normalized_percentile_')
    # convert_dtypes
    df_cnp = df_cnp.convert_dtypes()
    # join to df_work
    df_work = df_work.join(df_cnp)

    # apc_list
    df_apc_list = json_normalize(df_work_raw['apc_list'])
    df_apc_list['currency'].fillna('NO DATA', inplace=True)
    df_apc_list['provenance'].fillna('NO DATA', inplace=True)
    df_apc_list['value'].fillna(0, inplace=True)
    df_apc_list['value_usd'].fillna(0, inplace=True)
    df_apc_list = df_apc_list.add_prefix('apc_list_')
    df_apc_list = df_apc_list.convert_dtypes()
    df_work = df_work.join(df_apc_list)

    # apc_paid
    df_apc_paid = json_normalize(df_work_raw['apc_paid'])
    df_apc_paid['currency'].fillna('NO DATA', inplace=True)
    df_apc_paid['provenance'].fillna('NO DATA', inplace=True)
    df_apc_paid['value'].fillna(0, inplace=True)
    df_apc_paid['value_usd'].fillna(0, inplace=True)
    df_apc_paid = df_apc_paid.add_prefix('apc_paid_')
    df_apc_paid = df_apc_paid.convert_dtypes()
    df_work = df_work.join(df_apc_paid)

    # flat indexed_in
    df_work_indexed = pd.DataFrame()
    sources = ['arxiv', 'doaj', 'crossref', 'pubmed']
    for source in sources:
        df_work_indexed[f'indexed_in_{source}'] = df_work_raw['indexed_in'].apply(lambda x: source in x).astype(bool)

    df_work_indexed.reset_index(drop=True, inplace=True)

    df_work = pd.concat([df_work,df_work_indexed], axis=1)

    # rename de cols
    df_work.columns = df_work.columns.str.replace('.', '_')
    df_work.rename(columns={'id':'work_id'}, inplace=True)

    df_work['load_datetime'] = date.today()

    filter = df_work['work_id'].duplicated()
    duplicated_id = df_work[filter]['work_id']
    print('duplicated ids:\n',duplicated_id)

    duplicated_filter = df_work['work_id'].isin(duplicated_id)
    df_work[duplicated_filter]

    return df_work[~duplicated_filter], df_work[duplicated_filter]


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