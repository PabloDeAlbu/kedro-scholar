
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

def openaire_extract_researchproduct(filter_param, ror_filter_value, access_token, refresh_token, env):
    cursor = '*'
    organizations_base_url = 'https://api.openaire.eu/graph/v1/organizations'
    research_base_url = 'https://api.openaire.eu/graph/v2/researchProducts'
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

    org_query_params = {
        "pid": ror_filter_value,
    }

    # Resolver el id de OpenAIRE a partir del ROR
    while True:
        response = requests.get(organizations_base_url, headers=request_headers, params=org_query_params)

        if response.status_code == 403:
            if refresh_attempts >= max_refresh_attempts:
                raise Exception("Máximo de intentos para refrescar el token alcanzado. Abortando.")
            print("Access token expired or invalid while fetching organization. Refreshing token...")
            new_token = refresh_access_token(refresh_token)
            if not new_token:
                raise Exception("No se pudo refrescar el access token para organizations.")
            access_token = new_token
            request_headers["Authorization"] = f"Bearer {access_token}"
            refresh_attempts += 1
            continue

        if response.status_code != 200:
            raise Exception(f"Failed to fetch organization for ROR {ror_filter_value}: {response.status_code}")

        data = response.json()
        results = data.get("results", [])
        if not results:
            raise Exception(f"No organization found for ROR {ror_filter_value}")

        organization_id = results[0].get('id')
        if not organization_id:
            raise Exception("No se encontró id de organización en la respuesta.")

        print(f"OpenAIRE organization id resolved from ROR {ror_filter_value}: {organization_id}")
        break

    # Reiniciamos contador de refrescos antes de ir al endpoint de research products
    refresh_attempts = 0

    query_params = {
        filter_param: organization_id,
        "pageSize": page_size,
        "cursor": cursor
    }

    while True:
        response = requests.get(research_base_url, headers=request_headers, params=query_params)

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
                response = requests.get(research_base_url, headers=request_headers, params=query_params)

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

        df[filter_param] = organization_id

        df['load_datetime'] = date.today()

        return df, df.head(1000)
