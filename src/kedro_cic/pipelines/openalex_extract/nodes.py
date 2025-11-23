from datetime import date
import json
import requests
import pandas as pd
import time


def clean_openalex_institution(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza el campo "international" para evitar fallos al escribir Parquet."""
    df = df.copy()
    if "international" in df.columns:
        df["international"] = df["international"].apply(
            lambda x: None if not x else json.dumps(x, ensure_ascii=False)
        )
    return df

def openalex_extract(institution_ror: str, filter_field: str, entity: str = 'institutions', env: str = 'dev', cleaner=None):
    """
    Fetch data from OpenAlex API for a given entity and institution ROR.

    Args:
        entity (str): 'authors', 'institutions', 'works', etc.
        institution_ror (str): ROR id of the institution.
        env (str): 'dev' or 'prod'.
        filter_field (str): the filter key to use (e.g. 'affiliations.institution.ror').
        cleaner (callable): function to clean DataFrame columns, optional.

    Returns:
        pd.DataFrame: full concatenated results
        pd.DataFrame: head(1000) sample
    """
    session = requests.Session()
    base_url = f"https://api.openalex.org/{entity}?filter={filter_field}:{{}}&cursor={{}}&per-page=200"
    cursor = '*'
    iteration_limit = 1
    iteration_count = 0
    all_dataframes = []

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

        if 'results' not in api_response or not api_response['results']:
            print("No hay más datos disponibles.")
            break

        df_tmp = pd.DataFrame.from_dict(api_response['results'])
        if cleaner:
            df_tmp = cleaner(df_tmp)
        all_dataframes.append(df_tmp)

        # update cursor
        cursor = api_response.get('meta', {}).get('next_cursor')
        if not cursor:
            break

        iteration_count += 1
        if env == 'dev' and iteration_count >= iteration_limit:
            break

        time.sleep(1)

    df = pd.concat(all_dataframes, ignore_index=True) if all_dataframes else pd.DataFrame()
    df['extract_datetime'] = date.today()

    return df, df.head(1000)
