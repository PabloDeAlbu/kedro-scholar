import ast
from datetime import date
import math
import pandas as pd
import requests
import time

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

def refresh_access_token(refresh_token):
    """Obtiene un nuevo access_token usando el refresh_token."""
    refresh_url = f"https://services.openaire.eu/uoa-user-management/api/users/getAccessToken?refreshToken={refresh_token}"
    response = requests.get(refresh_url)
    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        raise Exception(f"Failed to refresh token: {response.status_code}")

def fetch_openaire_researchproduct_collectedfrom_datasource(relCollectedFromDatasourceId, access_token, refresh_token, env):
    cursor = '*'
    base_url = 'https://api.openaire.eu/graph/researchProducts'
    iteration_limit = 5
    iteration_count = 0
    page_size = 50  # Ajustar este valor según sea necesario
    max_retries = 5  # Máximo número de reintentos en caso de error 429
    retry_wait = 5   # Tiempo inicial de espera entre reintentos en segundos
    
    def get_headers():
        return {
            "accept": "application/json",
            'Authorization': f'Bearer {access_token}'
        }
    
    params = {
        "relCollectedFromDatasourceId": relCollectedFromDatasourceId,  # Búsqueda por institución
        "pageSize": page_size,
        "cursor": cursor
    }
    
    while True:
        response = requests.get(base_url, headers=get_headers(), params=params)
        
        # Si el token es inválido o expiró, intentar renovarlo
        if response.status_code == 403:
            print("Access token expired or invalid. Refreshing token...")
            access_token = refresh_access_token(refresh_token)
            continue  # Reintentar la solicitud con el nuevo token
        
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
            
            iteration_count += 1
            print(f'Iteration count: {iteration_count}')
            print(f'GET {response.url}')
            
            time.sleep(2)
            
            # Reintentos en caso de error 429
            retries = 0
            while retries < max_retries:
                response = requests.get(base_url, headers=get_headers(), params=params)
                
                if response.status_code == 403:
                    print("Access token expired during execution. Refreshing token...")
                    access_token = refresh_access_token(refresh_token)
                    continue  # Reintentar con el nuevo token
                
                if response.status_code == 429:
                    retries += 1
                    print(f"Rate limit hit. Retry {retries}/{max_retries}. Waiting {retry_wait} seconds...")
                    time.sleep(retry_wait)
                    retry_wait *= 2  # Incrementar el tiempo de espera exponencialmente
                else:
                    break
            
            if response.status_code != 200:
                print(f"Failed to retrieve data at iteration {iteration_count}: {response.status_code}")
                break
            
            api_response = response.json()
            
            if not api_response.get('results'):
                print("No more results. Stopping iteration.")
                break
            
            df_tmp = pd.DataFrame.from_dict(api_response['results'])
            df = pd.concat([df, df_tmp])
            
            cursor = api_response['header'].get('nextCursor', None)
            params["cursor"] = cursor
        
        return df, df.head(1000)
