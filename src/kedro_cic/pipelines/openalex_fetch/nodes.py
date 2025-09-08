from datetime import date
import pandas as pd
import requests
import time

def openalex_fetch_author(institution_ror, env):
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
        df['load_datetime'] = date.today()

    return df

def clean_work_dataframe(df):
    """Elimina columnas innecesarias si están presentes."""
    columns_to_drop = {"abstract_inverted_index", "abstract_inverted_index_v3"}
    return df.drop(columns=columns_to_drop.intersection(df.columns), inplace=False)

def openalex_fetch_work(institution_ror, env):
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

    df['load_datetime'] = date.today()

    return df, df.head(1000)
