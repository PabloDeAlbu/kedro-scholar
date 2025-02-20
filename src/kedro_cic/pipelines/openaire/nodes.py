from datetime import date
import pandas as pd
import requests
import time

def refresh_access_token(refresh_token):
    """Obtiene un nuevo access_token usando el refresh_token."""
    refresh_url = f"https://services.openaire.eu/uoa-user-management/api/users/getAccessToken?refreshToken={refresh_token}"
    response = requests.get(refresh_url)
    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        raise Exception(f"Failed to refresh token: {response.status_code}")

def fetch_openaire_researchproduct(filter_label, filter_param, filter_value, access_token, refresh_token, env):
    cursor = '*'
    base_url = 'https://api.openaire.eu/graph/researchProducts'
    iteration_limit = 5
    iteration_count = 0
    page_size = 50         # Ajustar según sea necesario
    max_retries = 5        # Máximo número de reintentos en caso de error 429
    retry_wait = 5         # Tiempo inicial de espera entre reintentos (segundos)
    max_refresh_attempts = 3  # Máximo número de intentos para refrescar el token
    refresh_attempts = 0

    request_headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }

    query_params = {
        filter_param: filter_value,
        "pageSize": page_size,
        "cursor": cursor
    }

    while True:
        response = requests.get(base_url, headers=request_headers, params=query_params)

        # Si el token es inválido o expiró, intentar renovarlo
        if response.status_code == 403:
            if refresh_attempts >= max_refresh_attempts:
                raise Exception("Máximo de intentos para refrescar el token alcanzado. Abortando.")
            print("Access token expired or invalid. Refreshing token...")
            new_token = refresh_access_token(refresh_token)
            if not new_token:
                raise Exception("No se pudo refrescar el access token.")
            access_token = new_token
            request_headers["Authorization"] = f"Bearer {access_token}"
            refresh_attempts += 1
            continue  # Reintenta la solicitud con el nuevo token

        if response.status_code != 200:
            raise Exception(f"Failed to retrieve data: {response.status_code}")

        # Restablecemos el contador de refrescos al tener una respuesta exitosa
        refresh_attempts = 0

        api_response = response.json()
        print(f"Iteration count: {iteration_count}")
        print(f"GET {response.url}")

        # Crear DataFrame con el primer bloque de resultados
        df = pd.DataFrame.from_dict(api_response["results"])

        # Actualizar cursor
        cursor = api_response["header"].get("nextCursor", None)
        query_params["cursor"] = cursor

        # Bucle para iterar con el cursor
        while cursor:
            if env == "dev" and iteration_count >= iteration_limit:
                break

            iteration_count += 1
            print(f"Iteration count: {iteration_count}")
            print(f"GET {response.url}")
            time.sleep(2)

            # Reintentos en caso de error 429
            retries = 0
            while retries < max_retries:
                response = requests.get(base_url, headers=request_headers, params=query_params)

                if response.status_code == 403:
                    if refresh_attempts >= max_refresh_attempts:
                        raise Exception("Máximo de intentos para refrescar el token alcanzado durante la ejecución. Abortando.")
                    print("Access token expired during execution. Refreshing token...")
                    new_token = refresh_access_token(refresh_token)
                    if not new_token:
                        raise Exception("No se pudo refrescar el access token durante la ejecución.")
                    access_token = new_token
                    request_headers["Authorization"] = f"Bearer {access_token}"
                    refresh_attempts += 1
                    continue  # Reintenta con el nuevo token

                if response.status_code == 429:
                    retries += 1
                    print(f"Rate limit hit. Retry {retries}/{max_retries}. Waiting {retry_wait} seconds...")
                    time.sleep(retry_wait)
                    retry_wait *= 2  # Incremento exponencial del tiempo de espera
                else:
                    break

            if response.status_code != 200:
                print(f"Failed to retrieve data at iteration {iteration_count}: {response.status_code}")
                break

            # Restablecer contador de refrescos tras respuesta exitosa
            refresh_attempts = 0

            api_response = response.json()

            if not api_response.get("results"):
                print("No more results. Stopping iteration.")
                break

            df_tmp = pd.DataFrame.from_dict(api_response["results"])
            df = pd.concat([df, df_tmp], ignore_index=True)

            cursor = api_response["header"].get("nextCursor", None)
            query_params["cursor"] = cursor

        # Suponiendo que se quiera marcar con la variable filter_value; de lo contrario, ajusta según corresponda.
        df['filter_applied'] = filter_label

        return df, df.head(1000)

def land_openaire_researchproduct(filter_value, df: pd.DataFrame)-> pd.DataFrame:

    df = df.convert_dtypes()

    expected_columns = [
        'filter_applied',
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
        'contactPerson',
        'coverage',
        'pid',
        'contactPerson',
        'embargoEndDate',
    ]

    # Agregar columnas faltantes con NaN
    for col in expected_columns:
        if col not in df.columns:
            df[col] = pd.NA

    df_researchproduct = df[expected_columns].copy()
    df.reset_index(drop=True, inplace=True)

    # language
    df_researchproduct['language_code'] = df_researchproduct['language'].apply(lambda x: x['code'])
    df_researchproduct['language_label'] = df_researchproduct['language'].apply(lambda x: x['label'])

    ## bestAccessRight
    df_researchproduct['bestAccessRight_label'] = df['bestAccessRight'].apply(lambda x: x['label'] if x else None)
    df_researchproduct['bestAccessRight_scheme'] = df['bestAccessRight'].apply(lambda x: x['scheme'] if x else None)

    ## indicators
    df_indicators = pd.json_normalize(df['indicators']).reset_index(drop=True)
    
    indicators_expected_columns = [
        "citationImpact.citationClass",
        "citationImpact.citationCount",
        "citationImpact.impulse",
        "citationImpact.impulseClass",
        "citationImpact.influence",
        "citationImpact.influenceClass",
        "citationImpact.popularity",
        "citationImpact.popularityClass",
        "usageCounts.downloads",
        "usageCounts.views",
    ]

    # Agregar columnas para indicators y faltantes con NaN
    for col in indicators_expected_columns:
        if col not in df_indicators.columns:
            df_indicators[col] = pd.NA

    df_researchproduct = pd.concat([df_researchproduct.drop(columns=['indicators']).reset_index(drop=True), df_indicators], axis=1)

    ## author
    df_researchproduct2author = df.explode('author').reset_index(drop=True)
    df_researchproduct2author = df_researchproduct2author[['id','author']]
    df_authors = pd.json_normalize(df_researchproduct2author['author']).reset_index(drop=True)
    df_researchproduct2author = pd.concat([df_researchproduct2author.drop(columns=['author']), df_authors], axis=1)

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
    
    # TODO contributor
    
    # TODO contactPerson

    # TODO coverage
    
    # pid
    df_researchproduct2pid = df.explode('pid').reset_index(drop=True)
    df_researchproduct2pid = df_researchproduct2pid[['id','pid']]
    df_pid = pd.json_normalize(df_researchproduct2pid['pid']).reset_index(drop=True)
    df_researchproduct2pid = pd.concat([df_researchproduct2pid.drop(columns=['pid']), df_pid], axis=1)

    # url
    df_researchproduct2instance = df.explode('instance').reset_index(drop=True)
    df_researchproduct2instance = df_researchproduct2instance[['id','instance']]
    df_instance = pd.json_normalize(df_researchproduct2instance['instance']).reset_index(drop=True)
    df_researchproduct2instance = pd.concat([df_researchproduct2instance.drop(columns=['instance']), df_instance], axis=1)
    df_researchproduct2url = df_researchproduct2instance[['id','url']]
    df_researchproduct2url = df_researchproduct2url.explode('url')

    ## drop de columnas procesadas en otros df
    df_researchproduct.drop(columns=[
        'author', 'country', 'subjects','bestAccessRight', 
        'language', 'format', 'instance', 'originalId', 
        'container', 'source', 'pid', 'description',
        'contributor', 'contactPerson', 'coverage'
        ], inplace=True)

    df_researchproduct['load_datetime'] = date.today()
    df_researchproduct2originalId['load_datetime'] = date.today()
    df_researchproduct2author['load_datetime'] = date.today()
    df_researchproduct2subject['load_datetime'] = date.today()
    df_researchproduct2pid['load_datetime'] = date.today()
    df_researchproduct2url['load_datetime'] = date.today()

    return df_researchproduct, df_researchproduct2originalId, df_researchproduct2author, df_researchproduct2subject, df_researchproduct2pid, df_researchproduct2url
