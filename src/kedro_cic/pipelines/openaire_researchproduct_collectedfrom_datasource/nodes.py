import ast
from datetime import date
import math
import pandas as pd
import requests
import time
import xmltodict

def fetch_openaire_researchproduct_collectedfrom_datasource(relCollectedFromDatasourceId, r_token, env):
    cursor = '*'
    base_url = 'https://api.openaire.eu/graph/researchProducts'
    iteration_limit = 5
    iteration_count = 0
    page_size = 50  # Ajustar este valor según sea necesario
    headers = {
        "accept": "application/json",
        'Authorization': f'Bearer {r_token}'
    }

    params = {
        "relCollectedFromDatasourceId": relCollectedFromDatasourceId,  # Búsqueda por institución
        "pageSize": page_size,
        "cursor": cursor
    }

    max_retries = 5  # Máximo número de reintentos en caso de error 429
    retry_wait = 5   # Tiempo inicial de espera entre reintentos en segundos

    # Primera solicitud
    response = requests.get(base_url, headers=headers, params=params)
    if response.status_code != 200:
        raise Exception(f"Failed to retrieve data: {response.status_code}")

    api_response = response.json()
    print(f'Iteration count: {iteration_count}')
    print(f'GET {response.url}')

    # Crear DataFrame con las columnas del primer resultado
    df = pd.DataFrame.from_dict(api_response['results'])

    # Actualizar cursor
    cursor = api_response['header'].get('nextCursor', None)
    params["cursor"] = cursor

    # Bucle para iterar con el cursor
    while cursor:
        if env == 'dev' and iteration_count >= iteration_limit:
            break

        # Actualizar iteración
        iteration_count += 1
        print(f'Iteration count: {iteration_count}')
        print(f'GET {response.url}')

        # Pausa para evitar sobrecargar la API
        time.sleep(2)

        # Reintentos en caso de error 429
        retries = 0
        while retries < max_retries:
            response = requests.get(base_url, headers=headers, params=params)
            if response.status_code == 429:
                retries += 1
                print(f"Rate limit hit. Retry {retries}/{max_retries}. Waiting {retry_wait} seconds...")
                time.sleep(retry_wait)
                retry_wait *= 2  # Incrementar el tiempo de espera exponencialmente
            else:
                break

        # Si aún después de reintentos no se resuelve, interrumpir
        if response.status_code != 200:
            print(f"Failed to retrieve data at iteration {iteration_count}: {response.status_code}")
            break

        api_response = response.json()
        
        # Verificar si 'results' está vacío
        if not api_response.get('results'):
            print("No more results. Stopping iteration.")
            break

        # Crear DataFrame temporal y concatenar
        df_tmp = pd.DataFrame.from_dict(api_response['results'])

        df = pd.concat([df, df_tmp])

        # Actualizar cursor
        cursor = api_response['header'].get('nextCursor', None)
        params["cursor"] = cursor

    return df, df.head(1000)


def land_openaire_researchproduct_collectedfrom_datasource(df: pd.DataFrame)-> pd.DataFrame:

    df = df.convert_dtypes()

    expected_columns = [
        'author',
        'openAccessColor',
        'publiclyFunded',
        'type',
        'language',
        'country',
        'subjects',
        'mainTitle',
        'description',
        'publicationDate',
        'format',
        'bestAccessRight',
        'id',
        'originalId',
        'indicators',
        'instance',
        'isGreen',
        'isInDiamondJournal',
        'publisher',
        'source',
        'container',
        'contributor',
        'pid',
        'contactPerson',
        'embargoEndDate'
    ]

    # Agregar columnas faltantes con NaN
    for col in expected_columns:
        if col not in df.columns:
            df[col] = pd.NA

    df_researchproduct = df[expected_columns].copy()
    df.reset_index(drop=True, inplace=True)

    ## author
    df_researchproduct2author = df.explode('author').reset_index(drop=True)
    df_researchproduct2author = df_researchproduct2author[['id','author']]
    df_authors = pd.json_normalize(df_researchproduct2author['author']).reset_index(drop=True)
    df_researchproduct2author = pd.concat([df_researchproduct2author.drop(columns=['author']), df_authors], axis=1)

    df_researchproduct['language_code'] = df_researchproduct['language'].apply(lambda x: x['code'])
    df_researchproduct['language_label'] = df_researchproduct['language'].apply(lambda x: x['label'])

    ## bestAccessRight
    df_researchproduct['bestAccessRight_label'] = df['bestAccessRight'].apply(lambda x: x['label'] if x else None)

    ## indicators
    df_indicators = pd.json_normalize(df['indicators']).reset_index(drop=True)
    df_researchproduct = pd.concat([df.drop(columns=['indicators']), df_indicators], axis=1)

    ## originalId
    df_researchproduct2originalId = df.explode('originalId').reset_index(drop=True)
    df_researchproduct2originalId = df_researchproduct2originalId[['id','originalId']]

    # TODO country

    ## subjects
    df_researchproduct2subject = df.explode('subjects').reset_index(drop=True)
    df_researchproduct2subject = df_researchproduct2subject[['id','subjects']]
    df_subjects = pd.json_normalize(df_researchproduct2subject['subjects']).reset_index(drop=True)
    df_researchproduct2subject = pd.concat([df_researchproduct2subject.drop(columns=['subjects']), df_subjects], axis=1)

    # TODO description

    # TODO format

    # TODO instance

    # TODO source

    # TODO container
    
    # TODO pid
    df_researchproduct2pid = df.explode('pid').reset_index(drop=True)
    df_researchproduct2pid = df_researchproduct2pid[['id','pid']]
    df_pid = pd.json_normalize(df_researchproduct2pid['pid']).reset_index(drop=True)
    df_researchproduct2pid = pd.concat([df_researchproduct2pid.drop(columns=['pid']), df_pid], axis=1)

    ## drop de columnas procesadas en otros df
    df_researchproduct.drop(columns=['author', 'country', 'subjects','bestAccessRight', 
                                     'language', 'format', 'instance', 'originalId', 
                                     'container', 'source', 'pid', 'description'], inplace=True)

    df_researchproduct['load_datetime'] = date.today()
    df_researchproduct2originalId['load_datetime'] = date.today()
    df_researchproduct2author['load_datetime'] = date.today()
    df_researchproduct2subject['load_datetime'] = date.today()
    df_researchproduct2pid['load_datetime'] = date.today()

    return df_researchproduct, df_researchproduct2originalId, df_researchproduct2author, df_researchproduct2subject, df_researchproduct2pid
