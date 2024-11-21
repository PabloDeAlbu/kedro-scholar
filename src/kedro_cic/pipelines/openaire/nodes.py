import ast
from datetime import date
import math
import pandas as pd
import requests
import xmltodict

def fetch_researchproduct_openaire(dim_doi: pd.DataFrame, r_token, env)-> pd.DataFrame:
    base_url = "https://api.openaire.eu/search/researchProducts"
    df_list = []

    doi_limit = 9999
    if (env == 'dev'): doi_limit = 9
    
    skipped_list = []

    not_in_openaire = dim_doi['in_openaire'] == False
    dim_doi = dim_doi[not_in_openaire]

    doi_list = dim_doi.iloc[0:doi_limit]['doi'].to_list()
    doi_comma_separated = ','.join(doi_list)

    # se define cantidad de batches a partir de la cantidad de resultados por batch y cantidad de doi
    batch_size = 10
    num_batches = math.ceil(len(doi_list) / batch_size)

    for batch_index in range(num_batches):

        batch = doi_list[batch_index * batch_size : (batch_index + 1) * batch_size]
        doi_comma_separated = ','.join(batch)

        graph_url = f"{base_url}?doi={doi_comma_separated}"
        headers = { 'Authorization': f'Bearer {r_token}' }

        api_response = requests.get(graph_url, headers=headers)
        print(f'GET "{graph_url}" {api_response.status_code}')

        if api_response.status_code == 200:
            data_dict = xmltodict.parse(api_response.content)
            results = data_dict.get('response', {}).get('results', {}).get('result', [])

            for result in results:

                publication_header = result.get('header', {})
                publication_metadata = result.get('metadata', {}).get('oaf:entity', {}).get('oaf:result', {})

                publication = publication_header | publication_metadata 
                if publication:
                    df_normalized = pd.json_normalize(publication, max_level=0)
                    df_list.append(df_normalized)
                else:
                    print("No publication data found in result")
        else:
            print(f'Error: Received status code {api_response.status_code}')
            skipped_list.extend(batch)
            break

    print(f'{len(df_list)} batches processed')
    print(f'{len(skipped_list)} DOIs skipped')

    if df_list:
        df = pd.concat(df_list, ignore_index=True)
    else:
        df = pd.DataFrame()

    return df

def land_researchproduct_openaire(df: pd.DataFrame)-> pd.DataFrame:

    ## Paso 1: Convierto tipos y selecciono columnas con cardinalidad 1 con respecto a cada research product
    df = df.convert_dtypes()
    df_researchproduct = df.loc[:,
    [
    #       '@xmlns:xsi', 
        'dri:objIdentifier', 
        'dri:dateOfCollection',
        'dri:dateOfTransformation', 
    #       'collectedfrom', 
    #       'originalId', 
    #       'pid',
    #       'measure', 
        'fulltext', 
    #       'title', 
    #       'bestaccessright', 
    #       'creator', 
    #       'country',
        'dateofacceptance', 
        'description', 
    #       'subject', 
    #       'language',
    #       'relevantdate', 
        'publisher', 
    #       'source', 
    #       'format', 
    #       'resulttype',
    #       'resourcetype', 
        'isgreen', 
        'openaccesscolor', 
        'isindiamondjournal',
        'publiclyfunded', 
    #       'journal', 
    #       'datainfo', 
    #       'rels', 
    #       'children', 
    #       'context',
    #       'contributor', 
    #       'embargoenddate', 
    #       'processingchargeamount',
    #       'processingchargecurrency', 
    #       'lastmetadataupdate', 
    #       'storagedate',
    #       'version'
    ]
    ]

    # Paso 2: Agrego fecha de carga
    df_researchproduct['load_datetime'] = date.today()

    return df_researchproduct

def land_researchproduct2measure_openaire(df: pd.DataFrame)-> pd.DataFrame:

    ## Paso 0: Seleccionar columnas con identificador y 'measure'
    df_researchproduct = df.loc[:, ['dri:objIdentifier', 'measure']]
    df_researchproduct = df_researchproduct.convert_dtypes()

    ## Paso 1: Asegurarse de que 'measure' sea un diccionario o lista
    df_researchproduct['measure'] = df_researchproduct['measure'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
    df_researchproduct['measure'] = df_researchproduct['measure'].apply(lambda x: [x] if not isinstance(x, list) else x)

    ## Paso 2: Explode la columna 'measure' y reinicia el índice
    df_researchproduct = df_researchproduct.explode('measure').reset_index(drop=True)

    ## Paso 3: Normalizar la columna 'measure' en nuevas columnas
    measure_expanded = pd.json_normalize(df_researchproduct["measure"])

    ## Paso 4: Concatenar asegurando que los índices están alineados
    df_researchproduct2measure = pd.concat([df_researchproduct, measure_expanded], axis=1)
    df_researchproduct2measure.drop(columns='measure', inplace=True)
    df_researchproduct['load_datetime'] = date.today()
    return df_researchproduct2measure


def land_researchproduct2pid_openaire(df: pd.DataFrame)-> pd.DataFrame:

    ## Paso 1: Seleccionar columnas con identificador y pid
    df_researchproduct = df.loc[:,['dri:objIdentifier', 'pid']]
    df_researchproduct = df_researchproduct.convert_dtypes()

    ## Paso 2: Asegurarse de que 'pid' sea un diccionario o lista
    df_researchproduct['pid'] = df_researchproduct['pid'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
    df_researchproduct['pid'] = df_researchproduct['pid'].apply(lambda x: [x] if not isinstance(x, list) else x)

    ## Paso 3: Explode la columna 'pid' y reinicia el índice
    df_researchproduct = df_researchproduct.explode('pid').reset_index(drop=True)

    ## Paso 4: Normalizar la columna 'pid' en nuevas columnas
    pid_expanded = pd.json_normalize(df_researchproduct["pid"])

    ## Paso 5: Eliminar columnas no deseadas de 'pid'
    pid_expanded.drop(columns=['@classname', '@schemeid', '@schemename', '@inferred', '@provenanceaction', '@trust'], inplace=True)

    ## Paso 6: Concatenar asegurando que los índices están alineados
    df_researchproduct2pid = pd.concat([df_researchproduct, pid_expanded], axis=1)
    df_researchproduct2pid.drop(columns='pid', inplace=True)

    ## Paso 7: Agrego load_datetime 
    df_researchproduct['load_datetime'] = date.today() 
    return df_researchproduct2pid
