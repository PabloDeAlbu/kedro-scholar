import pandas as pd
import requests
import time
from datetime import datetime
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

def clean_work_dataframe(df):
    """Elimina columnas innecesarias si están presentes."""
    columns_to_drop = {"abstract_inverted_index", "abstract_inverted_index_v3"}
    return df.drop(columns=columns_to_drop.intersection(df.columns), inplace=False)

def fetch_work_openalex(institution_ror, env):
    session = requests.Session()  # Reutilizar la sesión para eficiencia
    base_url = 'https://api.openalex.org/works?filter=institutions.ror:{}&cursor={}&per-page=200'
    cursor = '*'
    iteration_limit = 5
    iteration_count = 0
    all_dataframes = []  # Lista para almacenar los DataFrames antes de concatenar

    while True:
        url = base_url.format(institution_ror, cursor)
        print(f'Iteration count: {iteration_count}')
        print(f'GET {url}')

        try:
            response = session.get(url, timeout=10)
            response.raise_for_status()
            api_response = response.json()
        except requests.RequestException as e:
            print(f"Error en la solicitud: {e}")
            break
        except ValueError:
            print("Error al decodificar JSON.")
            break

        # Si no hay resultados, se termina el bucle
        if 'results' not in api_response or not api_response['results']:
            print("No hay más datos disponibles.")
            break

        df_tmp = pd.DataFrame.from_dict(api_response['results'])
        df_tmp = clean_work_dataframe(df_tmp)
        all_dataframes.append(df_tmp)

        # Actualizar cursor
        cursor = api_response.get('meta', {}).get('next_cursor')
        if not cursor:
            break

        # Control de iteraciones en entorno 'dev'
        iteration_count += 1
        if env == 'dev' and iteration_count >= iteration_limit:
            break

        time.sleep(1)  # Respetar límites de la API

    # Concatenar todos los DataFrames en uno solo
    df = pd.concat(all_dataframes, ignore_index=True) if all_dataframes else pd.DataFrame()

    return df, df.head(1000)

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
    df_author['load_datetime'] = pd.to_datetime(datetime.today())
    
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

    df_author2affiliation['load_datetime'] = pd.to_datetime(datetime.today())
    
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

    df_author2topic['load_datetime'] = pd.to_datetime(datetime.today())

    return df_author2topic

def land_work_openalex(df_work_raw):
    """Limpia y transforma los datos de OpenAlex para su almacenamiento en una base de datos relacional."""
    
    # Copia para evitar modificar el DataFrame original
    df_work = df_work_raw.copy()

    # Lista de columnas a eliminar
    columns_to_drop = {
        'authorships', 'institution_assertions', 'citation_normalized_percentile', 
        'cited_by_percentile_year', 'referenced_works', 'related_works', 'ids', 
        'primary_location', 'indexed_in', 'corresponding_author_ids', 'corresponding_institution_ids',
        'apc_list', 'apc_paid', 'biblio', 'primary_topic', 'topics', 'keywords', 
        'concepts', 'mesh', 'locations', 'best_oa_location', 'sustainable_development_goals',
        'grants', 'datasets', 'versions', 'abstract_inverted_index', 
        'abstract_inverted_index_v3', 'counts_by_year', 'open_access'
    }

    # Elimina solo las columnas que existen en el DataFrame
    df_work.drop(columns=columns_to_drop.intersection(df_work.columns), inplace=True)

    # Expandir la información de 'open_access' si está presente
    if 'open_access' in df_work.columns:
        df_openaccess_expanded = pd.json_normalize(df_work['open_access'].dropna())
        
        # Solo agrega las columnas si hay datos en open_access
        if not df_openaccess_expanded.empty:
            df_work = df_work.drop(columns=['open_access'])
            df_work = pd.concat([df_work, df_openaccess_expanded], axis=1)

    # Agregar la fecha de carga con formato datetime
    df_work['load_datetime'] = pd.to_datetime(datetime.today())

    # Convertir tipos de datos automáticamente
    df_work = df_work.convert_dtypes()

    return df_work

def land_work2apc_list_openalex(df_work_raw):
    df_work = df_work_raw.loc[:,['id','apc_list']]
    df_work = df_work.convert_dtypes()

    df_work2apc_list = pd.json_normalize(df_work['apc_list'])
    df_work2apc_list = pd.concat((df_work_raw.loc[:,'id'].reset_index(drop=True),df_work2apc_list), axis=1)
    
    df_work2apc_list['load_datetime'] = pd.to_datetime(datetime.today())

    return df_work2apc_list

def land_work2apc_paid_openalex(df_work_raw):
    df_work = df_work_raw.loc[:,['id','apc_paid']]
    df_work = df_work.convert_dtypes()

    df_work2apc_paid = pd.json_normalize(df_work['apc_paid'])
    df_work2apc_paid = pd.concat((df_work_raw.loc[:,'id'].reset_index(drop=True),df_work2apc_paid), axis=1)

    df_work2apc_paid['load_datetime'] = pd.to_datetime(datetime.today())

    return df_work2apc_paid

def land_work2authorships_openalex(df_work_raw):
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
    df_author2institution_exploded = df_work2authorships.explode('institutions', ignore_index=True)

    # Normalizar la información de instituciones
    df_institution_norm = pd.json_normalize(df_author2institution_exploded['institutions'])
    df_institution_norm.drop(columns=['lineage'], errors='ignore', inplace=True)

    # Combinar author_id con la información normalizada de instituciones
    df_author2institution = df_author2institution_exploded[['author_id']].join(df_institution_norm)

    df_work2author['load_datetime'] = pd.to_datetime(datetime.today())
    df_author2institution['load_datetime'] = pd.to_datetime(datetime.today())

    return df_work2author, df_author2institution

def land_work2ids_openalex(df_work_raw):
    df_work = df_work_raw.loc[:,['id','ids']]
    df_work = df_work.convert_dtypes()

    df_work2ids = pd.json_normalize(df_work['ids'])

    df_work2ids['load_datetime'] = pd.to_datetime(datetime.today())

    return df_work2ids

def land_work2concepts_openalex(df_work_raw):
    df_work = df_work_raw.loc[:,['id','concepts']]
    df_work = df_work.convert_dtypes()

    df_work2concepts_exploded = df_work.explode('concepts').reset_index(drop=True)
    df_work2concepts_norm = pd.json_normalize(df_work2concepts_exploded['concepts'])
    df_work2concepts_norm.rename(columns={'id':'concept_id'}, inplace=True)

    df_work = df_work2concepts_exploded.loc[:,'id']
    df_work2concepts = pd.concat((df_work, df_work2concepts_norm), axis=1)
    
    df_work2concepts['load_datetime'] = pd.to_datetime(datetime.today())

    return df_work2concepts

def land_work2corresponding_author_ids_openalex(df_work_raw):
    df_work = df_work_raw.loc[:,['id','corresponding_author_ids']]
    df_work = df_work.convert_dtypes()

    df_work2corresponding_author_ids = df_work.explode('corresponding_author_ids')

    df_work2corresponding_author_ids['load_datetime'] = pd.to_datetime(datetime.today())

    return df_work2corresponding_author_ids

def land_work2primary_topic_openalex(df_work_raw):
    df_work = df_work_raw.loc[:,['id','primary_topic']]
    df_work = df_work.convert_dtypes()
    df_work2primary_topic_norm = pd.json_normalize(df_work['primary_topic']).loc[:,['id','domain.id','field.id','subfield.id']]

    # 
    df_work2primary_topic_norm.rename(columns={'id':'topic.id'}, inplace=True)
    df_work2primary_topic = pd.concat((df_work['id'].reset_index(drop=True),df_work2primary_topic_norm), axis=1)

    df_work2primary_topic['load_datetime'] = pd.to_datetime(datetime.today())

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

    df_work2primarylocation['load_datetime'] = pd.to_datetime(datetime.today())

    return df_work2primarylocation

def land_work2referenced_works_openalex(df_work_raw):
    df_work = df_work_raw.loc[:,['id','referenced_works']]
    df_work = df_work.convert_dtypes()
    df_work2referenced_works_exploded =  df_work.explode('referenced_works')
    df_work2referenced_works = df_work2referenced_works_exploded.reset_index(drop=True)

    df_work2referenced_works['load_datetime'] = pd.to_datetime(datetime.today())

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

    df_work2topics['load_datetime'] = pd.to_datetime(datetime.today())

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

    df_work_location['load_datetime'] = pd.to_datetime(datetime.today())

    return df_work_location
