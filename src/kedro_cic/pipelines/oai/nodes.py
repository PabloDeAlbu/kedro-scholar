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

    print(f"Sleeping for {elapsed_time:.2f} seconds")
    time.sleep(elapsed_time)

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

def fetch_item_oai(base_url, context, env):
    
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

def land_item_oai(df_item_raw):

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