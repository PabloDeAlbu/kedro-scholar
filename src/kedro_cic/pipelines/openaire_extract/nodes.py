
from datetime import date
import pandas as pd
import requests
import time

def refresh_access_token(refresh_token):
    """Get a new access_token using the refresh_token."""
    refresh_url = f"https://services.openaire.eu/uoa-user-management/api/users/getAccessToken?refreshToken={refresh_token}"
    response = requests.get(refresh_url)
    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        raise Exception(f"Failed to refresh token: {response.status_code}")

def get_with_refresh(url, headers, params, refresh_token, max_refresh_attempts=3, context=""):
    attempts = 0
    current_access_token = headers.get("Authorization", "").replace("Bearer ", "")

    while True:
        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 403:
            return response, current_access_token

        if attempts >= max_refresh_attempts:
            raise Exception("Máximo de intentos para refrescar el token alcanzado. Abortando.")

        print(f"Access token expired or invalid {context}. Refreshing token...")
        new_token = refresh_access_token(refresh_token)
        if not new_token:
            raise Exception("No se pudo refrescar el access token.")

        current_access_token = new_token
        headers["Authorization"] = f"Bearer {current_access_token}"
        attempts += 1

def openaire_extract_researchproduct(filter_param, ror_filter_value, access_token, refresh_token, env):
    cursor = '*'
    organizations_base_url = 'https://api.openaire.eu/graph/v1/organizations'
    research_base_url = 'https://api.openaire.eu/graph/v2/researchProducts'
    iteration_limit = 2
    iteration_count = 0
    page_size = 50
    max_retries = 5
    retry_wait = 5
    max_refresh_attempts = 3

    request_headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }

    org_query_params = {
        "pid": ror_filter_value,
    }

    # Resolve OpenAIRE organization id from ROR
    while True:
        response, access_token = get_with_refresh(
            organizations_base_url,
            request_headers,
            org_query_params,
            refresh_token,
            max_refresh_attempts=max_refresh_attempts,
            context="while fetching organization",
        )

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

    query_params = {
        filter_param: organization_id,
        "pageSize": page_size,
        "cursor": cursor
    }

    while True:
        response, access_token = get_with_refresh(
            research_base_url,
            request_headers,
            query_params,
            refresh_token,
            max_refresh_attempts=max_refresh_attempts,
            context="while fetching research products",
        )

        if response.status_code != 200:
            raise Exception(f"Failed to retrieve data: {response.status_code}")

        api_response = response.json()
        print(f"Iteration count: {iteration_count}")
        print(f"GET {response.url}")

        # Build DataFrame with the first page of results
        df = pd.DataFrame.from_dict(api_response["results"])

        # Update cursor
        cursor = api_response["header"].get("nextCursor", None)
        query_params["cursor"] = cursor

        # Iterate pagination via cursor
        while cursor:
            if env == "dev" and iteration_count >= iteration_limit:
                break

            iteration_count += 1
            print(f"Iteration count: {iteration_count}")
            print(f"GET {response.url}")
            time.sleep(2)

            # Retries when rate limited (429)
            retries = 0
            while retries < max_retries:
                response, access_token = get_with_refresh(
                    research_base_url,
                    request_headers,
                    query_params,
                    refresh_token,
                    max_refresh_attempts=max_refresh_attempts,
                    context="during execution",
                )

                if response.status_code == 429:
                    retries += 1
                    print(f"Rate limit hit. Retry {retries}/{max_retries}. Waiting {retry_wait} seconds...")
                    time.sleep(retry_wait)
                    retry_wait *= 2  # Exponential backoff
                else:
                    break

            if response.status_code != 200:
                print(f"Failed to retrieve data at iteration {iteration_count}: {response.status_code}")
                break

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
