import os
import time
import certifi
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from requests.packages.urllib3.exceptions import InsecureRequestWarning


def get_oai_response(base_url, verify=None, max_retries=3, backoff_factor=1.0, min_interval=0.0):

    # Usa el bundle de certifi para evitar errores de certificado en requests
    os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())
    os.environ.setdefault("SSL_CERT_FILE", certifi.where())
    VERIFY_SSL = os.getenv("OAI_VERIFY_SSL", "false").lower() == "true"
    CA_BUNDLE = os.getenv("OAI_CA_BUNDLE") or certifi.where()
    requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

    verify_param = CA_BUNDLE if VERIFY_SSL else False
    if verify is not None:
        verify_param = verify

    for attempt in range(1, max_retries + 1):
        start_time = time.time()
        response = None
        error = None
        try:
            response = requests.get(base_url, verify=verify_param)
        except requests.RequestException as exc:
            error = exc
        elapsed_time = time.time() - start_time

        if min_interval > 0:
            wait_time = max(min_interval - elapsed_time, 0)
            if wait_time > 0:
                print(f"Pausando {wait_time:.2f} segundos para no saturar el servidor")
                time.sleep(wait_time)

        if error:
            print(f"Error en request (intento {attempt}/{max_retries}): {error}")

        if response and response.status_code == 200:
            return response

        status = response.status_code if response else "sin respuesta"
        print(f"Error: {status} (intento {attempt}/{max_retries})")

        if attempt < max_retries:
            backoff = backoff_factor * attempt
            print(f"Reintentando en {backoff:.2f} segundos...")
            time.sleep(backoff)
    return None

def log_oai_progress(token_elem, total_processed: int):
    """Muestra el avance usando completeListSize y los registros acumulados."""
    if token_elem is None:
        return
    total = token_elem.get('completeListSize')
    try:
        total_int = int(total) if total is not None else None
        if total_int is not None and total_processed is not None:
            remaining = total_int - total_processed
            print(f"Progreso OAI: {total_processed}/{total_int} (faltan ~{remaining})")
    except ValueError:
        # Si el servidor devuelve valores no numéricos, ignora el progreso.
        pass

def oai_extract_identifiers_by_sets(base_url: str, context: str, env: str, df_set: pd.DataFrame, iteration_limit = 1, verify=None) -> pd.DataFrame:
    records = []
    if env == "dev": iteration_limit = 2 

    col_ids = df_set.loc[:, "setSpec"].tolist()

    for set_id in col_ids:
        iteration_count = 0
        resumption_token = f'oai_dc///{set_id}/0'

        while True:
            if iteration_count >= iteration_limit:
                break

            params = f'/{context}?verb=ListIdentifiers&resumptionToken={resumption_token}'
            url = base_url + params

            print(f"Consultando: {url}")

            response = get_oai_response(url, verify=verify)
            if not response or not response.ok:
                print(f"Error al consultar: {url}")
                break
            
            iteration_count += 1

            xml_content = response.text

            root = ET.fromstring(xml_content)
            ns = { 'oai': 'http://www.openarchives.org/OAI/2.0/' }
        
            record_nodes = root.findall('.//oai:header', ns)

            if not record_nodes:
                print("No se encontraron más registros.")
                break

            for record in record_nodes:
                
                # Valores simples
                record_id = record.find('.//oai:identifier', ns)
                record_datestamp = record.find('.//oai:datestamp', ns)
                
                # Multivaluados
                setspec = [e.text for e in record.findall('.//oai:setSpec', ns)]

                records.append({
                    'record_id': record_id.text if record_id is not None else None,
                    'datestamp': record_datestamp.text if record_datestamp is not None else None,
                    'set_id': setspec,
                })

            token_elem = root.find('.//oai:resumptionToken', ns)
            if token_elem is not None:
                complete_list_size = int(token_elem.get('completeListSize'))
                resumption_token = token_elem.text

            # guarda el tamaño en el df de sets
            df_set.loc[df_set["setSpec"] == set_id, "completeListSize"] = (
                int(complete_list_size) if complete_list_size is not None else None
            )
          
    df = pd.DataFrame(records)

    timestamp = pd.Timestamp.now(tz="UTC").normalize()
    df['extract_datetime'] = timestamp

    return df, df_set, df.head(100)

def oai_extract_records_by_identifiers(base_url: str, context: str, env: str, df_ids: pd.DataFrame, iteration_limit = 1, verify=None) -> pd.DataFrame:
    records = []
    ids_limit = 2 if env == "dev" else None
    ids = df_ids.head(ids_limit).loc[:, "id"].tolist()

    for record_id in ids:
        params = f'/{context}?verb=GetRecord&metadataPrefix=oai_dc&identifier={record_id}'
        url = base_url + params

        print(f"Consultando: {url}")

        response = get_oai_response(url, verify=verify)
        if not response or not response.ok:
            print(f"Error al consultar: {url}")
            continue

        xml_content = response.text

        root = ET.fromstring(xml_content)
        ns = { 'oai': 'http://www.openarchives.org/OAI/2.0/' ,
               'dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/'
            }
    
        record_nodes = root.findall('.//oai:header', ns)

        if not record_nodes:
            print("No se encontraron más registros.")
            continue

        for record in record_nodes:
            
            # Valores simples
            record_identifier_node = record.find('.//oai:identifier', ns)
            
            record_datestamp = record.find('.//oai:datestamp', ns)
            
            # Multivaluados
            setspec = [e.text for e in record.findall('.//oai:setSpec', ns)]

            record_date = record.find('.//dc:date', ns)
            record_title = record.find('.//dc:title', ns)

            record_creator = [e.text for e in record.findall('.//dc:creator', ns)]
            record_subject = [e.text for e in record.findall('.//dc:subject', ns)]
            record_description = [e.text for e in record.findall('.//dc:description', ns)]
            record_type = [e.text for e in record.findall('.//dc:type', ns)]
            record_identifier = [e.text for e in record.findall('.//dc:identifier', ns)]
            record_language = [e.text for e in record.findall('.//dc:language', ns)]
            record_relation = [e.text for e in record.findall('.//dc:relation', ns)]
            record_rights = [e.text for e in record.findall('.//dc:rights', ns)]
            record_format = [e.text for e in record.findall('.//dc:format', ns)]
            record_publisher = [e.text for e in record.findall('.//dc:publisher', ns)]
            
            records.append({
                'record_id': record_identifier_node.text if record_identifier_node is not None else None,
                'record_date': record_date.text if record_date is not None else None,
                'record_title': record_title.text if record_title is not None else None,
                'datestamp': record_datestamp.text if record_datestamp is not None else None,

                'set_id': setspec,
                'record_creator': record_creator,
                'record_subject': record_subject,
                'record_description': record_description,
                'record_type': record_type,
                'record_identifier': record_identifier,
                'record_language': record_language,
                'record_relation': record_relation,
                'record_rights': record_rights,
                'record_format': record_format,
                'record_publisher': record_publisher
            })
          
    df = pd.DataFrame(records)

    timestamp = pd.Timestamp.now(tz="UTC").normalize()
    df['extract_datetime'] = timestamp

    # convierte cada lista en columnas (set_0, set_1, ...)
    sets_df = df['set_id'].apply(pd.Series)
    sets_df = sets_df.rename(columns=lambda i: f'set_{i}')

    # junta con record_id y (opcional) elimina la columna original
    df_sets = pd.concat([df[['record_id']], sets_df], axis=1)

    return df, df_sets, df.head(100)


def oai_extract_records(base_url: str, context: str, env: str, verify=None) -> pd.DataFrame:
    records = []
    
    iteration_limit = 2 if env == "dev" else None
    resumption_token = None
    iteration_count = 0

    total_processed = 0

    while True:
        if iteration_limit is not None and iteration_count >= iteration_limit:
            break

        if resumption_token:
            params = f'/{context}?verb=ListRecords&resumptionToken={resumption_token}'
        else:
            params = f'/{context}?verb=ListRecords&metadataPrefix=oai_dc'

        url = base_url + params

        print(f"Consultando: {url}")

        response = get_oai_response(url, verify=verify)

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
            header = record.find('.//oai:header', ns)
            identifier_node = header.find('.//oai:identifier', ns) if header is not None else None
            datestamp_node = header.find('.//oai:datestamp', ns) if header is not None else None
            setspec = [e.text for e in header.findall('.//oai:setSpec', ns)] if header is not None else []

            metadata = record.find('.//oai:metadata', ns)

            if metadata is None:
                continue

            # Valores simples
            title = metadata.find('.//dc:title', ns)
            date_issued = metadata.find('.//dc:date', ns)

            # Multivaluados
            setspec = [e.text for e in record.findall('.//oai:setSpec', ns)]

            creators = [e.text for e in metadata.findall('.//dc:creator', ns)]
            types = [e.text for e in metadata.findall('.//dc:type', ns)]
            identifiers = [e.text for e in metadata.findall('.//dc:identifier', ns)]
            languages = [e.text for e in metadata.findall('.//dc:language', ns)]
            publishers = [e.text for e in metadata.findall('.//dc:publisher', ns)]
            subjects = [e.text for e in metadata.findall('.//dc:subject', ns)]
            relations = [e.text for e in metadata.findall('.//dc:relation', ns)]
            rights = [e.text for e in metadata.findall('.//dc:rights', ns)]

            records.append({
                'record_id': identifier_node.text if identifier_node is not None else None,
                'datestamp': datestamp_node.text if datestamp_node is not None else None,
                'set_id': setspec,
                'col_id': setspec[0] if setspec else None,
                'title': title.text if title is not None else None,
                'date_issued': date_issued.text if date_issued is not None else None,
                'creators': creators,
                'types': types,
                'identifiers': identifiers,
                'languages': languages,
                'subjects': subjects,
                'publishers': publishers,
                'relations': relations,
                'rights': rights
            })

        total_processed += len(record_nodes)

        token_elem = root.find('.//oai:resumptionToken', ns)
        resumption_token = token_elem.text if token_elem is not None else None
        log_oai_progress(token_elem, total_processed)

        if not resumption_token:
            break

    df = pd.DataFrame(records)

    timestamp = pd.Timestamp.now(tz="UTC").normalize()
    df['extract_datetime'] = timestamp

    return df, df.head(100)

def oai_extract_sets(base_url, context, env, verify=None, iteration_limit=None):

    if iteration_limit is None and env == "dev":
        iteration_limit = 2

    resumption_token = 0
    all_sets = []

    iteration_count = 0

    while True:

        if iteration_limit is not None and iteration_count >= iteration_limit:
            break

        params = f'/{context}?verb=ListSets&resumptionToken=////{resumption_token}'
        url = base_url + params

        print(f"Consultando: {url}")

        response = get_oai_response(url, verify=verify)
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

    timestamp = pd.Timestamp.now(tz="UTC").normalize()
    df_sets['extract_datetime'] = timestamp

    return df_sets

def oai_intermediate_sets(df_sets):
    
    df_sets["is_col_set"] = df_sets["setSpec"].str.startswith("col_")
    df_sets["is_com_set"] = df_sets["setSpec"].str.startswith("com_")

    return df_sets

def oai_filter_col(df_sets, env):
    
    col_filter = df_sets["is_col_set"] == True
    df_col = df_sets[col_filter]#.loc[:, "setSpec"]

    if env == "dev":
        df_col = df_col.head(2)
    
    return df_col
