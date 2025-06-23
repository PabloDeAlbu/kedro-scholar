import requests
import pandas as pd
import xml.etree.ElementTree as ET
import time
from datetime import date

def get_oai_records(base_url):
    start_time = time.time()

    response = requests.get(base_url)
    end_time = time.time()
    elapsed_time = end_time - start_time

    # Esperar el doble del tiempo de la solicitud + un delay fijo (ej. 1 segundo)
    delay = max(2 * elapsed_time, 1.0)  # Al menos 1 segundo de espera
    print(f"Sleeping for {delay:.2f} seconds")
    time.sleep(delay)

    if response.status_code == 200:
        return response
    else:
        print(f"Error: {response.status_code}")
        return None

def parse_oai_response(xml_data):
    root = ET.fromstring(xml_data)
    namespaces = {'oai': 'http://www.openarchives.org/OAI/2.0/',
                  'dc': 'http://purl.org/dc/elements/1.1/'}

    records = []
    for record in root.findall('.//oai:record', namespaces):
        metadata = record.find('.//oai:metadata', namespaces)
        if metadata is not None:
            record_dict = {}
            for elem in metadata.findall('.//dc:*', namespaces):
                tag = elem.tag.split('}')[-1]
                if tag in record_dict:
                    record_dict[tag].append(elem.text)
                else:
                    record_dict[tag] = [elem.text]
            records.append(record_dict)

    return records


# Helper to safely get nth element from lists or tuples

def get_nth(x, n):
    if isinstance(x, (list, tuple)) and len(x) > n:
        return x[n]
    return None

# Helper to explode a column into its own dataframe

def explode_column(df_raw, col, new_col_name):
    df = df_raw[['handle', col]] \
        .dropna(subset=[col]) \
        .explode(col) 
    df = df.rename(columns={col: new_col_name})
    df['load_datetime'] = date.today()
    return df

def fetch_oai_items(base_url, context, env):
    
    resumption_token = '0'
    params = f'/{context}?verb=ListRecords&resumptionToken=oai_dc////'
    url = base_url + params + str(resumption_token)
    items = []
    resumption_token = 0
    url = base_url + params + str(resumption_token)

    response = get_oai_records(url)
    records = parse_oai_response(response.text)
    record_size = len(records) 
    items.extend(records)

    iteration_limit = 2
    iteration_count = 0

    while record_size > 0:

        if env == 'dev' and iteration_count >= iteration_limit:
            break

        record_size = len(records)
        resumption_token += 100
        url = base_url + params + str(resumption_token)
        print(url)
        response = get_oai_records(url)
        records = parse_oai_response(response.text)
        items.extend(records)
        iteration_count += 1

    df = pd.DataFrame(items)
    return df

def fetch_oai_item_by_set(base_url: str, context: str, set_id: str, metadata_format: str, env: str) -> pd.DataFrame:
    records = []
    resumption_token = 0
    iteration_limit = 2
    iteration_count = 0
   
    while True:
        if env == 'dev' and iteration_count >= iteration_limit:
            break

        params = f'/{context}?verb=ListRecords&resumptionToken={metadata_format}///{set_id}/{resumption_token}'
        url = base_url + params
        
        print(f"Consultando: {url}")
        
        response = get_oai_records(url)

        resumption_token += 100
        iteration_count += 1

        if not response or not response.ok:
            print(f"Error al consultar: {url}")
            break

        xml_content = response.text
        root = ET.fromstring(xml_content)
        ns = {
            'oai': 'http://www.openarchives.org/OAI/2.0/',
            'dc': 'http://purl.org/dc/elements/1.1/'
        }

        record_nodes = root.findall('.//oai:record', ns)


        if not record_nodes:
            print("No se encontraron más registros.")
            break

        for record in record_nodes:
            identifier = record.find('.//oai:identifier', ns)
            item_id = identifier.text if identifier is not None else None
            metadata = record.find('.//oai:metadata', ns)

            if metadata is None:
                continue

            # Valores simples
            title = metadata.find('.//dc:title', ns)
            date = metadata.find('.//dc:date', ns)

            # Multivaluados
            creators = [e.text for e in metadata.findall('.//dc:creator', ns)]
            types = [e.text for e in metadata.findall('.//dc:type', ns)]
            identifiers = [e.text for e in metadata.findall('.//dc:identifier', ns)]
            languages = [e.text for e in metadata.findall('.//dc:language', ns)]
            publishers = [e.text for e in metadata.findall('.//dc:publisher', ns)]
            subjects = [e.text for e in metadata.findall('.//dc:subject', ns)]
            relations = [e.text for e in metadata.findall('.//dc:relation', ns)]
            rights = [e.text for e in metadata.findall('.//dc:rights', ns)]

            records.append({
                'item_id': item_id,
                'col_id': set_id,
                'title': title.text if title is not None else None,
                'date': date.text if date is not None else None,
                'creators': creators,
                'types': types,
                'identifiers': identifiers,
                'languages': languages,
                'subjects': subjects,
                'publishers': publishers,
                'relations': relations,
                'rights': rights
            })

    df = pd.DataFrame(records)

    return df, df.head(100)

def fetch_oai_sets(base_url, context, env):

    resumption_token = 0
    all_sets = []

    iteration_limit = 2
    iteration_count = 0

    while True:

        if env == 'dev' and iteration_count >= iteration_limit:
            break

        params = f'/{context}?verb=ListSets&resumptionToken=////{resumption_token}'
        url = base_url + params

        print(f"Consultando: {url}")

        response = get_oai_records(url)
        if not response:
            break

        xml_content = response.text
        root = ET.fromstring(xml_content)
        ns = {'oai': 'http://www.openarchives.org/OAI/2.0/'}

        sets_data = []
        for set_elem in root.findall('.//oai:set', ns):
            set_spec = set_elem.find('oai:setSpec', ns).text if set_elem.find('oai:setSpec', ns) is not None else None
            set_name = set_elem.find('oai:setName', ns).text if set_elem.find('oai:setName', ns) is not None else None
            sets_data.append({'setSpec': set_spec, 'setName': set_name})

        if not sets_data:
            print("No se encontraron más sets.")
            break

        all_sets.extend(sets_data)
        resumption_token += 100  # avanzar manualmente
        iteration_count += 1

    df_sets = pd.DataFrame(all_sets)
    return df_sets

def land_items_oai(df_item_raw):

    df_item = pd.DataFrame()
    df_item['title'] = df_item_raw['title'].apply(lambda x: x[0] if x is not None and len(x) > 0 else None)
    df_item['title_alternative'] = df_item_raw['title'].apply(lambda x: x[1] if x is not None and len(x) > 1 else None)
    #df_item['handle'] = df_item_raw['identifier'].apply(lambda x: x[0] if x is not None and len(x) > 0 else None)
    #df_item['doi'] = df_item_raw['identifier'].apply(lambda x: x[1] if x is not None and len(x) > 1 else None)

    # Convertir la columna 'identifier' en un DataFrame, expandiendo cada elemento de la lista en una columna
    identifiers_df = df_item_raw['identifier'].apply(pd.Series)

    # Renombrar las columnas para que sean 'identifier_0', 'identifier_1', etc.
    identifiers_df.columns = [f'identifier_{i}' for i in range(identifiers_df.shape[1])]

    # Unir el DataFrame original con el nuevo DataFrame de identificadores
    df_item = pd.concat([df_item, identifiers_df], axis=1)

    df_item['date_issued'] = df_item_raw['date'].apply(lambda x: x[0] if x is not None and len(x) > 0 else None)
    df_item['date_exposure'] = df_item_raw['date'].apply(lambda x: x[1] if x is not None and len(x) > 1 else None)
    df_item['description'] = df_item_raw['description'].apply(lambda x: x[0] if x is not None and len(x) > 0 else None)
    df_item['type_openaire'] = df_item_raw['type'].apply(lambda x: x[0] if x is not None and len(x) > 0 else None)
    df_item['type_snrd'] = df_item_raw['type'].apply(lambda x: x[1] if x is not None and len(x) > 1 else None)
    df_item['version'] = df_item_raw['type'].apply(lambda x: x[2] if x is not None and len(x) > 2 else None)
    df_item['access_right'] = df_item_raw['rights'].apply(lambda x: x[0] if x is not None and len(x) > 0 else None)
    df_item['access_level'] = df_item_raw['rights'].apply(lambda x: x[0] if x is not None and len(x) > 0 else None)
    df_item['license_condition'] = df_item_raw['rights'].apply(lambda x: x[1] if x is not None and len(x) > 1 else None)

    df_item_creator = pd.DataFrame()
    df_item_creator['item_identifier'] = df_item_raw['identifier'].apply(lambda x: x[0] if x is not None and len(x) > 0 else None)
    df_item_creator['creator'] = df_item_raw['creator']
    df_item_creator.dropna(inplace=True)
    df_item_creator = df_item_creator.explode('creator')
    df_item_creator.rename(columns={'creator':'creator_name'}, inplace=True)

    df_item_contributor = pd.DataFrame()
    df_item_contributor['item_identifier'] = df_item_raw['identifier'].apply(lambda x: x[0] if x is not None and len(x) > 0 else None)
    df_item_contributor['contributor'] = df_item_raw['contributor']
    df_item_contributor.dropna(inplace=True)
    df_item_contributor = df_item_contributor.explode('contributor')
    df_item_contributor.rename(columns={'contributor':'contributor_name'}, inplace=True)

    df_item_language = pd.DataFrame()
    df_item_language['item_identifier'] = df_item_raw['identifier'].apply(lambda x: x[0] if x is not None and len(x) > 0 else None)
    df_item_language['language'] = df_item_raw['language']
    df_item_language.dropna(inplace=True)
    df_item_language = df_item_language.explode('language')
    df_item_language.rename(columns={'language':'language_iso'}, inplace=True)

    df_item_subject = pd.DataFrame()
    df_item_subject['item_identifier'] = df_item_raw['identifier'].apply(lambda x: x[0] if x is not None and len(x) > 0 else None)
    df_item_subject['subject'] = df_item_raw['subject']
    df_item_subject.dropna(inplace=True)
    df_item_subject = df_item_subject.explode('subject')
    df_item_subject.rename(columns={'subject':'subject_name'}, inplace=True)

    df_item_relation = pd.DataFrame()
    df_item_relation['item_identifier'] = df_item_raw['identifier'].apply(lambda x: x[0] if x is not None and len(x) > 0 else None)
    df_item_relation['relation'] = df_item_raw['relation']
    df_item_relation = df_item_relation.explode('relation')
    df_item_relation.dropna(inplace=True)
    df_item_relation['relation'] = df_item_relation['relation'].apply(lambda x: x.replace('info:eu-repo/semantics/altIdentifier/url/', ''))
    df_item['load_datetime'] = date.today()
    df_item_creator['load_datetime'] = date.today()
    df_item_contributor['load_datetime'] = date.today()
    df_item_language['load_datetime'] = date.today()
    df_item_subject['load_datetime'] = date.today()
    df_item_relation['load_datetime'] = date.today()
    df_item_relation.rename(columns={'relation':'relation_uri'}, inplace=True)

    return df_item, df_item_creator, df_item_contributor, df_item_language, df_item_subject, df_item_relation

from datetime import date
import pandas as pd

# Helper to safely get nth element from lists or tuples

def get_nth(x, n):
    try:
        if x is not None and len(x) > n:
            return x[n]
    except Exception:
        pass
    return None

# Helper to explode a column into its own dataframe

def explode_column(df_raw, col, new_col_name):
    df = df_raw[['handle', col]] \
        .dropna(subset=[col]) \
        .explode(col) 
    df = df.rename(columns={col: new_col_name})
    df['load_datetime'] = date.today()
    return df



def land_oai_item_by_set(df_item_raw):
    # Compute handle once
    df = df_item_raw.copy()
    df['handle'] = df_item_raw['identifiers'].apply(lambda x: x[0] if x is not None and len(x) > 0 else None)

    # Main item table
    df_item = pd.DataFrame({
        'item_id': df['item_id'],
        'handle':      df['handle'],
        'col_id':      df['col_id'],
        'title':       df['title'],
        'date_issued': df['date'],
        'type_openaire':    df['types'].apply(lambda x: get_nth(x, 0)),
        'type_snrd':        df['types'].apply(lambda x: get_nth(x, 1)),
        'version':          df['types'].apply(lambda x: get_nth(x, 2)),
        'access_right':     df['rights'].apply(lambda x: get_nth(x, 0)),
        'license_condition':df['rights'].apply(lambda x: get_nth(x, 1)),
    })
    df_item['load_datetime'] = date.today()

    # Explode multivalued fields
    df_item_creator   = explode_column(df, 'creators',  'creator')
    df_item_language  = explode_column(df, 'languages', 'language_iso')
    df_item_subject   = explode_column(df, 'subjects',  'subject')
    df_item_publisher = explode_column(df, 'publishers','publisher')
    df_item_relation  = explode_column(df, 'relations','relation')
    
    # Clean up relation URLs
    df_item_relation['relation'] = df_item_relation['relation'] \
        .str.replace('info:eu-repo/semantics/altIdentifier/url/', '', regex=False)

    return (
        df_item,
        df_item_creator,
        df_item_language,
        df_item_subject,
        df_item_relation,
        df_item_publisher
    )
