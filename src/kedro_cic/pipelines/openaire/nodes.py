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

def fetch_openaire_researchproduct(filter_param, filter_value, access_token, refresh_token, env):
    cursor = '*'
    base_url = 'https://api.openaire.eu/graph/v1/researchProducts'
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

        df[filter_param] = filter_value

        return df, df.head(1000)

def land_openaire_rel_researchproduct_authors(df: pd.DataFrame)-> pd.DataFrame:

    df_research_author = df[['id','authors']].explode('authors').reset_index(drop=True)

    df_authors = pd.json_normalize(df_research_author['authors'])

    df_research_author = pd.concat([df_research_author['id'], df_authors], axis=1)

    df_research_author['load_datetime'] = date.today()

    return df_research_author

def land_openaire_rel_researchproduct_instances(df: pd.DataFrame)-> pd.DataFrame:
    
    df_research_instances = df[['id','instances']]
    
    df_research_instances = df_research_instances.explode('instances').reset_index(drop=True)

    df_instances = pd.json_normalize(df_research_instances['instances'])
    df_research_instances = pd.concat([df_research_instances['id'], df_instances], axis=1)
    
    df_research_instances = df_research_instances.explode('pids').reset_index(drop=True)

    df_research_instances = df_research_instances.explode('urls').reset_index(drop=True)

    df_pids = pd.json_normalize(df_research_instances['pids'])
    df_research_instances = df_research_instances.drop(columns=['pids'])

    df_research_instances = pd.concat([df_research_instances, df_pids], axis=1)

    
    df_research_alternateidentifiers = df_research_instances[['id','alternateIdentifiers']].dropna().explode('alternateIdentifiers').reset_index(drop=True)
    df_alternateidentifiers = pd.json_normalize(df_research_alternateidentifiers['alternateIdentifiers'])
    df_research_alternateidentifiers = pd.concat([df_research_alternateidentifiers['id'], df_alternateidentifiers], axis=1)

    df_research_instances.drop(columns=['alternateIdentifiers'], inplace=True)

    df_research_instances['load_datetime'] = date.today()
    df_research_alternateidentifiers['load_datetime'] = date.today()

    return df_research_instances, df_research_alternateidentifiers

def land_openaire_rel_researchproduct_originalid(df: pd.DataFrame)-> pd.DataFrame:

    df_research_originalids = df[['id','originalIds']]

    df_research_originalids = df_research_originalids.explode('originalIds').reset_index(drop=True)

    df_research_originalids['load_datetime'] = date.today()
    
    return df_research_originalids

def land_openaire_rel_researchproduct_pids(df: pd.DataFrame)-> pd.DataFrame:

    df_research_pid = df[['id','pids']]

    df_research_pid = df_research_pid.explode('pids').reset_index(drop=True)

    df_pid = pd.json_normalize(df_research_pid['pids'])

    df_research_pid = pd.concat([df_research_pid['id'], df_pid], axis=1)

    df_research_pid.dropna(inplace=True)
    
    df_research_pid['load_datetime'] = date.today()

    return df_research_pid

def land_openaire_rel_researchproduct_sources(df: pd.DataFrame)-> pd.DataFrame:

    df_research_sources = df[['id','sources']]
    df_research_sources = df_research_sources.explode('sources').reset_index(drop=True)

    df_research_sources['load_datetime'] = date.today()

    return df_research_sources

def land_openaire_rel_researchproduct_subjects(df: pd.DataFrame)-> pd.DataFrame:

    df_research_subjects = df[['id','subjects']]

    df_research_subjects = df_research_subjects.explode('subjects').reset_index(drop=True)

    df_subjects = pd.json_normalize(df_research_subjects['subjects'])
    df_research_subjects = pd.concat([df_research_subjects['id'], df_subjects],axis=1)

    df_research_subjects['load_datetime'] = date.today()

    return df_research_subjects

def land_openaire_researchproduct(filter_param, filter_value, df: pd.DataFrame)-> pd.DataFrame:

    expected_columns = [
        'id',
        'openAccessColor',
        'publiclyFunded',
        'type',
        'language',
        'country',
        'mainTitle',
        'description',
        'publicationDate',
        'format',
        'bestAccessRight',
        'indicators',
        'isGreen',
        'isInDiamondJournal',
        'publisher',
        'source',
        'container',
        'contributor',
        'contactPerson',
        'coverage',
        'contactPerson',
        'embargoEndDate',
    ]

    # Agregar columnas faltantes con NaN
    for col in expected_columns:
        if col not in df.columns:
            df[col] = pd.NA

    df = df.convert_dtypes()

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

    # TODO country
    # TODO description
    # TODO format
    # TODO instance
    # TODO source
    # TODO container
    # TODO contributor
    # TODO contactPerson
    # TODO coverage

    ## drop de columnas procesadas en otros df
    df_researchproduct.drop(columns=[
        'country', 'bestAccessRight', 
        'language', 'format',  
        'container', 'source', 'description',
        'contributor', 'contactPerson', 'coverage'
        ], inplace=True)

    df_researchproduct['load_datetime'] = date.today()

    df_researchproduct[filter_param] = filter_value

    return df_researchproduct
