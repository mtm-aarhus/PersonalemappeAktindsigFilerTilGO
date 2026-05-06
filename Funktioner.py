from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
import json
import requests
import smtplib
from email.message import EmailMessage
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from requests_ntlm import HttpNtlmAuth
import openpyxl
import io
import re
import pandas as pd
from datetime import datetime
import time
import os
from urllib.parse import unquote, urlparse
from robot_framework import config
import uuid
import xml.etree.ElementTree as ET
SMTP_SERVER = "smtp.adm.aarhuskommune.dk"
SMTP_PORT = 25
SCREENSHOT_SENDER = "personaleindsigt@aarhus.dk"

def create_case(go_api_url, SagsTitel, AktID, session):
    '''
    Function for creating case in GetOrganized for the applicant to access
    '''
    url = f"{go_api_url}/aktindsigt/_goapi/Cases"

    payload = json.dumps({
    "CaseTypePrefix": "AKT",
    "MetadataXml": f"<z:row xmlns:z=\"#RowsetSchema\" ows_Title=\"Aktindsigtssag {AktID} - {SagsTitel}\" ows_CaseStatus=\"Åben\" />",
    "ReturnWhenCaseFullyCreated": True
    })
    headers = {
    'Content-Type': 'application/json'
    }

    response = session.post(url, headers=headers, data=payload)


    return response.text

def upload_document_go(go_api_url, payload, session):
    '''
    Uploades document to case in GO
    '''
    url = f"{go_api_url}/_goapi/Documents/AddToCase"
    response = session.post(url, data=payload, timeout=1200)
    response.raise_for_status()
    return response.json()


def delete_case_go(go_api_url, session, sagsnummer):
    '''
    Deletes case in go
    '''
    url = f"{go_api_url}/aktindsigt/_goapi/Cases/{sagsnummer}"
    response = session.delete(url, data= {"Data": ""}, timeout=1200)
    response.raise_for_status()
    return response.json()

def send_ingen_doko_mail(SagsID, ModtagerMail, orchestrator_connection):
    mailtekst = f"""
            <p style="color: #b91c1c; margin-top: 16px;">
                <strong>Aktliste kan ikke genereres - tjek at dokumentlisterne er udfyldt korrekt.
            </p>
        """
    msg = EmailMessage()
    msg['To'] = ModtagerMail
    msg['From'] = SCREENSHOT_SENDER
    msg['Subject'] = f"Sag nr. {SagsID}: Aktliste kunne ikke oprettes"
    UdviklerMail = orchestrator_connection.get_constant('balas').value
    msg['Reply-To'] = UdviklerMail
    msg['Bcc'] = UdviklerMail
    msg.set_content("Please enable HTML to view this message.")
    msg.add_alternative(mailtekst, subtype='html')

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as smtp:
            smtp.send_message(msg)
    except Exception as e:
        print(e)

def send_succes_email(SagsID, ModtagerMail, Url, orchestrator_connection, ikke_konverterede_filer, fejlede_uploads=None):
   
    UdviklerMail = orchestrator_connection.get_constant('balas').value

    subject = f"Sag nr. {SagsID}: Dokumenterne er overført til GO"

    if ikke_konverterede_filer:
        filliste_sektion = f"""
            <p style="color: #b91c1c; margin-top: 16px;">
                <strong>Bemærk:</strong> Følgende filer kunne ikke konverteres til PDF 
                og skal eventuelt behandles manuelt:
            </p>
            <ul>
                {"".join(f"<li>{fil}</li>" for fil in ikke_konverterede_filer)}
            </ul>
        """
    else:
        filliste_sektion = ""

    if fejlede_uploads:
        fejl_sektion = f"""
            <p style="color: #b91c1c; margin-top: 16px;">
                <strong>Fejl:</strong> Følgende filer kunne ikke uploades til GO og skal 
                overføres manuelt:
            </p>
            <ul>
                {"".join(f"<li>{fil}</li>" for fil in fejlede_uploads)}
            </ul>
        """
    else:
        fejl_sektion = ""

    html = f"""
    <html>
    <body>
        <p>Dokumenterne, der er angivet som 'Ja' eller 'Delvis' i dokumentlisterne 
        er overført til GO.</p>
        <p>Du kan se sagen og gennemgå dokumenterne inden udlevering på linket herunder:</p>
        <a href="{Url}">Link til sagen</a>
        {filliste_sektion}
        {fejl_sektion}
    </body>
    </html>
    """

    msg = EmailMessage()
    msg['To'] = ModtagerMail
    msg['From'] = SCREENSHOT_SENDER
    msg['Subject'] = subject
    msg['Reply-To'] = UdviklerMail
    msg['Bcc'] = UdviklerMail
    msg.set_content("Please enable HTML to view this message.")
    msg.add_alternative(html, subtype='html')

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as smtp:
            smtp.send_message(msg)
    except Exception as e:
        print(e)

def create_session (Username, PasswordString):
    # Create a session
    session = requests.Session()
    session.auth = HttpNtlmAuth(Username, PasswordString)
    return session

def parse_dato_ddmmåååå(navn):
    match = re.search(r"(\d{2})-(\d{2})-(\d{4})", navn)
    if match:
        try:
            return datetime.strptime(match.group(0), "%d-%m-%Y")
        except ValueError:
            return None
    return None

def hent_dokumenttitler_nyeste_filer(site_url, relative_root_folder_url, brugernavn, kodeord, orchestrator_connection):
    certification = orchestrator_connection.get_credential("SharePointCert")
    api = orchestrator_connection.get_credential("SharePointAPI")

    cert_credentials = {
        "tenant": api.username,
        "client_id": api.password,
        "thumbprint": certification.username,
        "cert_path": certification.password
    }

    ctx = ClientContext(site_url).with_client_certificate(**cert_credentials)

    orchestrator_connection.log_info(f'Opretter forbindelse til SharePoint: {site_url}')
    orchestrator_connection.log_info(f'Relative URL: {relative_root_folder_url}')

    try:
        orchestrator_connection.log_info('Henter root-mappe...')
        folder = ctx.web.get_folder_by_server_relative_url(relative_root_folder_url)
        ctx.load(folder)
        ctx.execute_query()
        orchestrator_connection.log_info(f"Fundet mappe: {folder.properties['Name']}")
    except Exception as e:
        orchestrator_connection.log_info(f"Fejl ved hentning af root-mappe: {e}")
        return [], []

    try:
        orchestrator_connection.log_info('Henter undermapper...')
        subfolders = folder.folders
        ctx.load(subfolders)
        ctx.execute_query()
        orchestrator_connection.log_info(f'Undermapper hentet')
    except Exception as e:
        orchestrator_connection.log_info(f"Fejl ved hentning af undermapper: {e}")
        return [], []

    DokumentTitler = []
    DokIDer = []
    DokLinks = []
    aktliste_rows = []
    AktIDer = []
    UnderMappeNavne = []

    subfolders_list = list(subfolders)
    orchestrator_connection.log_info(f'Antal undermapper fundet: {len(subfolders_list)}')

    for i, sf in enumerate(subfolders_list):
        orchestrator_connection.log_info(f'Behandler undermappe {i+1}/{len(subfolders_list)}...')
        ctx.load(sf)
        ctx.execute_query()
        undermappe_navn = sf.properties.get("Name", "")
        orchestrator_connection.log_info(f'Undermappe navn: {undermappe_navn}')

        try:
            orchestrator_connection.log_info(f'Henter filer i {undermappe_navn}...')
            files = sf.files
            ctx.load(files)
            ctx.execute_query()
            orchestrator_connection.log_info(f'Filer hentet i {undermappe_navn}')
        except Exception as e:
            orchestrator_connection.log_info(f"Kunne ikke hente filer i {undermappe_navn}: {e}")
            continue

        filer_med_dato = []
        for fil in files:
            filnavn = fil.properties["Name"]
            if filnavn.endswith((".xlsx", ".xls")):
                dato = parse_dato_ddmmåååå(filnavn)
                if dato:
                    filer_med_dato.append((dato, fil))

        orchestrator_connection.log_info(f'Excel-filer med dato i {undermappe_navn}: {len(filer_med_dato)}')
        if not filer_med_dato:
            continue

        nyeste_fil = max(filer_med_dato, key=lambda x: x[0])[1]
        tmp_path = os.path.join(os.path.expanduser("~"), "Downloads", f"_tmp_aktliste_{undermappe_navn}.xlsx")
        try:
            server_relative_url = nyeste_fil.properties["ServerRelativeUrl"]
            orchestrator_connection.log_info(f'Åbner fil: {server_relative_url}')
            response = File.open_binary(ctx, server_relative_url)
            orchestrator_connection.log_info(f'Fil hentet ({len(response.content)} bytes), læser Excel...')

            if len(response.content) == 0:
                orchestrator_connection.log_info(f'Fil er tom (0 bytes) - springer over')
                continue

            with open(tmp_path, "wb") as f:
                f.write(response.content)

            df = pd.read_excel(tmp_path, engine="openpyxl")
            orchestrator_connection.log_info(f'DataFrame oprettet: {len(df)} rækker, {len(df.columns)} kolonner')

            if df.empty:
                orchestrator_connection.log_info(f'Ark har ingen datarækker - springer over')
                continue

            doklink_kol = [c for c in df.columns if str(c) == "Link til dokument"]
            if doklink_kol:
                try:
                    wb = openpyxl.load_workbook(tmp_path, data_only=True, read_only=False, keep_vba=False, keep_links=False)
                    ws = wb.active
                    kol_index = df.columns.get_loc(doklink_kol[0])
                    for ri, row in enumerate(ws.iter_rows(min_row=2, max_row=len(df)+1)):
                        cell = row[kol_index] if kol_index < len(row) else None
                        if cell and cell.hyperlink:
                            df.at[ri, doklink_kol[0]] = cell.hyperlink.target
                    wb.close()
                except Exception as e:
                    orchestrator_connection.log_info(f'Kunne ikke hente hyperlinks: {e}')

        except Exception as e:
            orchestrator_connection.log_info(f"Kunne ikke læse Excel-fil: {e}")
            continue
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

        aktindsigt_kol = [c for c in df.columns if "Gives der aktindsigt" in c]
        dokumenttitel_kol = [c for c in df.columns if "Dokumenttitel" in c]
        dokid_kol = [c for c in df.columns if str(c) == "Dok ID"]
        doklink_kol = [c for c in df.columns if str(c) == "Link til dokument"]
        aktid_kol = [c for c in df.columns if "Akt ID" in c]
        kategori_kol = [c for c in df.columns if "Dokumentkategori" in c]
        dato_kol = [c for c in df.columns if "Dokumentdato" in c]
        bilagtil_kol = [c for c in df.columns if "Bilag til Dok ID" in c]
        bilag_kol = [c for c in df.columns if str(c) == "Bilag"]
        omfattet_kol = [c for c in df.columns if "omfattet" in str(c).lower()]
        aktindsigt_kol = [c for c in df.columns if "gives der aktindsigt" in str(c).lower()]
        begrundelse_kol = [c for c in df.columns if "Begrundelse hvis nej eller delvis" in c]

        orchestrator_connection.log_info(f'Kolonner fundet - aktindsigt: {bool(aktindsigt_kol)}, titel: {bool(dokumenttitel_kol)}, dokid: {bool(dokid_kol)}, doklink: {bool(doklink_kol)}, omfattet: {bool(omfattet_kol)}')

        if aktindsigt_kol and dokumenttitel_kol and dokid_kol and doklink_kol and omfattet_kol:
            kolonne = aktindsigt_kol[0]
            maske = df[kolonne].astype(str).str.lower().str.strip().str.contains("ja|delvis", na=False)
            filtreret = df[maske]

            titler = filtreret[dokumenttitel_kol[0]].dropna().tolist()
            dokider = filtreret[dokid_kol[0]].dropna().tolist()
            doklinks = filtreret[doklink_kol[0]].dropna().tolist()
            aktider = filtreret[aktid_kol[0]].dropna().tolist()
            undermappe_navne = [undermappe_navn] * len(titler)

            DokumentTitler.extend(titler)
            DokIDer.extend(dokider)
            DokLinks.extend(doklinks)
            AktIDer.extend(aktider)
            UnderMappeNavne.extend(undermappe_navne)

            maske_aktliste = df[omfattet_kol[0]].astype(str).str.lower().str.strip().str.contains("ja", na=False)
            aktliste_filtreret = df[maske_aktliste]

            for _, row in aktliste_filtreret.iterrows():
                akt_id_val = "" if not aktid_kol or pd.isna(row.get(aktid_kol[0])) else str(row.get(aktid_kol[0]))
                dok_id_val = "" if not dokid_kol or pd.isna(row.get(dokid_kol[0])) else str(row.get(dokid_kol[0]))

                if "." in akt_id_val:
                    akt_id_val = akt_id_val.split(".")[0]
                if "." in dok_id_val:
                    dok_id_val = dok_id_val.split(".")[0]

                row_dict = {
                    "Akt ID": akt_id_val,
                    "Filnavn": row.get(dokumenttitel_kol[0], "") if dokumenttitel_kol else "",
                    "Kategori": row.get(kategori_kol[0], "") if kategori_kol else "",
                    "Dato": row.get(dato_kol[0], "") if dato_kol else "",
                    "Dok ID": dok_id_val,
                    "Bilag til Dok ID": row.get(bilagtil_kol[0], "") if bilagtil_kol else "",
                    "Bilag": row.get(bilag_kol[0], "") if bilag_kol else "",
                    "Omfattet af aktindsigt?": row.get(omfattet_kol[0], "") if omfattet_kol else "",
                    "Gives der aktindsigt?": row.get(aktindsigt_kol[0], "") if aktindsigt_kol else "",
                    "Begrundelse hvis Nej/Delvis": row.get(begrundelse_kol[0], "") if begrundelse_kol else "",
                    "Link til dokument": row.get(doklink_kol[0], "") if doklink_kol else ""
                }

                if any(v not in [None, "", float('nan')] for v in row_dict.values()):
                    aktliste_rows.append(row_dict)
        else:
            orchestrator_connection.log_info(f"Mangler nødvendige kolonner i {undermappe_navn} eller tomme.")

    orchestrator_connection.log_info(f'Dokumentliste færdig. Fandt {len(DokumentTitler)} dokumenter på tværs af alle undermapper.')
    return list(zip(DokumentTitler, DokIDer, DokLinks, AktIDer, UnderMappeNavne)), aktliste_rows

def download_file(file_path_without_ext, DokumentID, GOUrl, GoUsername, GoPassword):
    try:
        max_retries = 2
        for attempt in range(max_retries):
            try:
                # Hent metadata for at finde dokumentets URL
                metadata_url = f"{GOUrl}/_goapi/Documents/MetadataWithSystemFields/{DokumentID}"
                metadata_response = requests.get(
                    metadata_url,
                    auth=HttpNtlmAuth(GoUsername, GoPassword),
                    headers={"Content-Type": "application/json"},
                    timeout=60
                )

                content = metadata_response.text
                DocumentURL = content.split("ows_EncodedAbsUrl=")[1].split('"')[1]
                DocumentURL = DocumentURL.split("\\")[0].replace("go.aarhus", "ad.go.aarhus")

                # Hent filendelse fra URL
                file_path = file_path_without_ext  # fallback

                # Download selve filen
                handler = requests.Session()
                handler.auth = HttpNtlmAuth(GoUsername, GoPassword)
                with handler.get(DocumentURL, stream=True) as download_response:
                    download_response.raise_for_status()
                    with open(file_path, "wb") as file:
                        for chunk in download_response.iter_content(chunk_size=8192):
                            file.write(chunk)

                break

            except Exception as retry_exception:
                print(f"Retry {attempt + 1} failed: {retry_exception}")
                if attempt == max_retries - 1:
                    raise RuntimeError(
                        f"Failed to download file after {max_retries} retries. "
                        f"DokumentID: {DokumentID}, Path: {file_path_without_ext}"
                    )
                time.sleep(5)

    except RuntimeError as nested_exception:
        print(f"An unrecoverable error occurred: {nested_exception}")
        raise nested_exception

def delete_local_file(filsti):
    try:
        os.remove(filsti)
    except FileNotFoundError:
        pass  # filen blev aldrig skrevet til disk — forventet i rute 1 og 2
    except Exception as e:
        raise Exception(f"Uventet fejl ved sletning af {filsti}: {e}")

def make_payload_document(ows_dict: dict, caseID: str, FolderPath: str, byte_arr: list, filename):
    ows_str = ' '.join([f'ows_{k}="{v}"' for k, v in ows_dict.items()])
    MetaDataXML = f'<z:row xmlns:z="#RowsetSchema" {ows_str}/>'

    return {
        "Bytes": byte_arr,
        "CaseId": caseID,
        "ListName": "Dokumenter",
        "FolderPath": FolderPath.replace("\\","/"),
        "FileName": filename,
        "Metadata": MetaDataXML,
        "Overwrite": True
    }

def fetch_document_info_go(api_url, DokumentID, session, AktID, Titel):
    url = f"{api_url}/_goapi/Documents/Data/{DokumentID}"
    response = session.get(url)
    data = json.loads(response.text)
    item_properties = data.get("ItemProperties", "")
    file_type_match = re.search(r'ows_File_x0020_Type="([^"]+)"', item_properties)
    version_ui_match = re.search(r'ows__UIVersionString="([^"]+)"', item_properties)
    DokumentType = file_type_match.group(1) if file_type_match else "unknown"
    VersionUI = version_ui_match.group(1) if version_ui_match else "Not found"
    file_title = f"{AktID} - {DokumentID} - {Titel}"
    return {"DokumentType": DokumentType, "VersionUI": VersionUI, "file_title": file_title}

def fetch_document_bytes(api_url, session, DokumentID, file_path=None, max_retries=30, retry_interval=5):
    url = f"{api_url}/_goapi/Documents/DocumentBytes/{DokumentID}"
    ByteResult = None
    for attempt in range(max_retries):
        try:
            response = session.get(url, timeout=180)
            response.raise_for_status()
            if b"HTTP Error 503. The service is unavailable." in response.content:
                print(f"Attempt {attempt + 1}: 503 fejl")
                time.sleep(retry_interval)
                continue
            ByteResult = response.content
            break
        except Exception as e:
            print(f"Attempt {attempt + 1}: {e}")
            time.sleep(retry_interval)
    if file_path and ByteResult:
        with open(file_path, "wb") as f:
            f.write(ByteResult)
    return ByteResult

def GOPDFConvert(api_url, DokumentID, VersionUI, GoUsername, GoPassword):
    try:
        url = f"{api_url}/_goapi/Documents/ConvertToPDF/{DokumentID}/{VersionUI}"
        response = requests.get(
            url,
            auth=HttpNtlmAuth(GoUsername, GoPassword),
            headers={"Content-Type": "application/json"},
            timeout=None
        )
        if "Document could not be converted" in response.text:
            return None
        return response.content
    except Exception:
        return None
def try_convert_go_file_to_pdf(api_url, DokumentID, session, GoUsername, GoPassword, GOUrl, file_path, orchestrator_connection=None):
    metadata = fetch_document_info_go(api_url, DokumentID, session, 0, "temp")
    VersionUI = metadata["VersionUI"]
    DokumentType = metadata["DokumentType"]
    titel = os.path.basename(file_path)

    if DokumentType.lower() == "pdf":
        if orchestrator_connection:
            orchestrator_connection.log_info(f"{DokumentID} er allerede PDF")
        byte_result = fetch_document_bytes(api_url, session, DokumentID)
        return byte_result, True, None

    # 1️⃣ Forsøg GO konvertering
    result = GOPDFConvert(api_url, DokumentID, VersionUI, GoUsername, GoPassword)
    if result:
        if orchestrator_connection:
            orchestrator_connection.log_info(f"{DokumentID} konverteret via GO")
        return result, True, None

    # 2️⃣ Forsøg fetch_document_bytes
    if orchestrator_connection:
        orchestrator_connection.log_info(f"{DokumentID} GO-konvertering fejlede, forsøger fetch_document_bytes")
    byte_result = fetch_document_bytes(api_url, session, DokumentID, file_path=file_path)
    if byte_result:
        return byte_result, False, titel

    # 3️⃣ Forsøg download via metadata-URL
    if orchestrator_connection:
        orchestrator_connection.log_info(f"{DokumentID} fetch_document_bytes fejlede, forsøger metadata-URL")
    try:
        download_file(file_path, DokumentID, GOUrl, GoUsername, GoPassword)
        with open(file_path, "rb") as f:
            byte_result = f.read()
        return byte_result, False, titel
    except Exception as e:
        if orchestrator_connection:
            orchestrator_connection.log_info(f"{DokumentID} alle download-metoder fejlede: {e}")
        return None, False, titel
    
#Below is for uploading large/failed files
def chunked_file_upload(APIURL, case_url, binary, file_name, session, request_digest, folder_path, orchestrator_connection: OrchestratorConnection):
    orchestrator_connection.log_info(f'Folder path: {folder_path}')
    orchestrator_connection.log_info(f'File name: {file_name}')
    chunk_size_bytes = 1024 * 10240
    session.headers.update({
        'X-FORMS_BASED_AUTH_ACCEPTED': 'f',
        'X-RequestDigest': request_digest
    })
    orchestrator_connection.log_info(request_digest)

    web_url = APIURL+"/"+case_url
    if folder_path is not None:
        target_folder_url = f"/{case_url}/Dokumenter/{folder_path}".replace("\\", "/")
    else:
        target_folder_url = f"/{case_url}/Dokumenter"
        
    create_file_request_url = f"{web_url}/_api/web/GetFolderByServerRelativePath(DecodedUrl=@p)/Files/add(url=@f,overwrite=true)?@p='{target_folder_url}'&@f='{file_name}'"

    response = session.post(create_file_request_url)
    response.raise_for_status()  # Ensure file creation is successful

    target_url = f"{target_folder_url}%2F{file_name}"

    upload_id = str(uuid.uuid4())  # Unique upload session ID
    offset = 0
    total_size = len(binary)

    with io.BytesIO(binary) as input_stream:
        first_chunk = True

        while True:
            buffer = input_stream.read(chunk_size_bytes)
            if not buffer:
                break  # End of file reached

            if first_chunk and len(buffer) == total_size:
                # If the file fits in a single chunk, handle it differently
                # StartUpload and FinishUpload in one step
                endpoint_url = f"{web_url}/_api/web/GetFileByServerRelativePath(DecodedUrl=@u)/startUpload(uploadId=guid'{upload_id}')?@u='{target_url}'"
                orchestrator_connection.log_info(endpoint_url)
                response = session.post(endpoint_url, data=buffer)
                response.raise_for_status()

                endpoint_url =  f"{web_url}/_api/web/GetFileByServerRelativePath(DecodedUrl=@u)/finishUpload(uploadId=guid'{upload_id}',fileOffset={offset})?@u='{target_url}'"
         
                orchestrator_connection.log_info(endpoint_url)
                response = session.post(endpoint_url, data=buffer)
                response.raise_for_status()
                break  # Upload complete
            elif first_chunk:
                # StartUpload: Initiating the upload session for large files
                endpoint_url = f"{web_url}/_api/web/GetFileByServerRelativePath(DecodedUrl=@u)/startUpload(uploadId=guid'{upload_id}')?@u='{target_url}'"

                orchestrator_connection.log_info(endpoint_url)
                response = session.post(endpoint_url, data=buffer)
                response.raise_for_status()
                first_chunk = False
            elif input_stream.tell() == total_size:
                # FinishUpload: Upload the final chunk for large files
                endpoint_url = f"{web_url}/_api/web/GetFileByServerRelativePath(DecodedUrl=@u)/finishUpload(uploadId=guid'{upload_id}',fileOffset={offset})?@u='{target_url}'"
                orchestrator_connection.log_info(endpoint_url)
                response = session.post(endpoint_url, data=buffer)
                response.raise_for_status()
            else:
                # ContinueUpload: Upload subsequent chunks
                endpoint_url = f"{web_url}/_api/web/GetFileByServerRelativePath(DecodedUrl=@u)/continueUpload(uploadId=guid'{upload_id}',fileOffset={offset})?@u='{target_url}'"
                orchestrator_connection.log_info(endpoint_url)
                response = session.post(endpoint_url, data=buffer)
                response.raise_for_status()

            offset += len(buffer)
            chunk_uploaded(offset, total_size, orchestrator_connection)  # Callback for tracking progress

def request_form_digest(APIURL, case_url, session: requests.session):
    endpoint_url = f"{APIURL}/{case_url}/_api/contextinfo"
    session.headers.update({
        'Accept': 'application/json; odata=verbose'
    })
    response = session.post(endpoint_url)
    response.raise_for_status()
    data = response.json()
    return data['d']['GetContextWebInformation']['FormDigestValue']

def get_docid(file_name, APIURL, case_url, folder_path, session: requests.session, orchestrator_connection: OrchestratorConnection):
    orchestrator_connection.log_info(f'Fetching doc_id for {file_name}')

    sags_url = f'{APIURL}/{case_url}/_goapi/Administration/GetLeftMenuCounter'

    # Make the GET request using the session
    response = session.get(sags_url)
    response.raise_for_status()
    data = response.json()

    ViewId = None
    for item in data:
        if item.get("ViewName") == "AllItems.aspx" and item.get("ListName") == "Dokumenter":
            ViewId = item.get("ViewId")
            break

    if ViewId is None:
        raise ValueError(f"ViewId for AllItems.aspx not found.")


    list_url = f"'/{case_url}/Dokumenter'"
    if folder_path is None:
        root_folder = f"/{case_url}/Dokumenter"
    else:
        folder_path = folder_path.replace("''", "'")
        root_folder = f"/{case_url}/Dokumenter/{folder_path}"

    headers = {
        'content-type': 'application/json;odata=verbose'
    }

    url = f"{APIURL}/{case_url}/_api/web/GetList(@listUrl)/RenderListDataAsStream?@listUrl={list_url}&View={ViewId}&RootFolder={root_folder}"

    while True:
        payload_dict = {
            "parameters": {
                "__metadata": {
                    "type": "SP.RenderListDataParameters"
                },
                "ViewXml": (
                    "<View>"
                    "<Query>"
                    "<Where>"
                    "<Eq>"
                    "<FieldRef Name=\"UniqueId\" />"
                    f"<Value Type=\"Guid\">{str(uuid.uuid4())}</Value>"
                    "</Eq>"
                    "</Where>"
                    "</Query>"
                    "<RowLimit Paged=\"TRUE\">100</RowLimit>"
                    "</View>"
                )
            }
        }

        payload = json.dumps(payload_dict)

        response = session.post(url, headers=headers, data=payload)
        response.raise_for_status()

        data = response.json()

        for row in data.get('Row', []):
            if str(row.get('FileLeafRef')).lower() == str(file_name).lower():
                orchestrator_connection.log_info(f'DocID: {row.get("DocID")}')
                return row.get('DocID')

        next_href = data.get('NextHref')
        if next_href:
            next_href = next_href.replace("?", "&", 1)
            url = f"{APIURL}/{case_url}/_api/web/GetList(@listUrl)/RenderListDataAsStream?@listUrl={list_url}{next_href}"
            orchestrator_connection.log_info(f"Fetching next page: {url}")
        else:
            orchestrator_connection.log_info("DocID not found.")
            return None 

# Example usage
def chunk_uploaded(offset, total_size, orchestrator_connection: OrchestratorConnection):
    orchestrator_connection.log_info(f"Uploaded {offset} out of {total_size} bytes")

def get_case_type(APIURL, session, case_id):
    response = session.get(f"{APIURL}/_goapi/Cases/Metadata/{case_id}/False")
    # Parse the XML data in Metadata
    metadata = response.json()["Metadata"]

    # Parse the XML string and find the 'row' element
    root = ET.fromstring(metadata)
    case_url = root.attrib.get('ows_CaseUrl')
    return case_url

def update_metadata(APIURL, docid, session, metadata, orchestrator_connection: OrchestratorConnection):
    # Find the part of the string that contains ows_Dato
    start_index = metadata.find('ows_Dato="') + len('ows_Dato="')
    end_index = metadata.find('"', start_index)

    # Extract the date value
    date_str = metadata[start_index:end_index]

    # Split the date by '-'
    day, month, year = date_str.split('-')

    # Construct the new date in mm-dd-yyyy format
    flipped_date = f'{month}-{day}-{year}'

    # Replace the original date with the new one in the metadata string
    metadata = metadata.replace(date_str, flipped_date)

    payload = {"DocId": docid,
               "MetadataXml": metadata}

    response = session.post(f'{APIURL}/_goapi/Documents/Metadata', data=payload, timeout=600)
    response.raise_for_status()

def upload_large_document(APIURL, payload, session, binary, orchestrator_connection: OrchestratorConnection):
    case_id = payload["CaseId"]
    folder_path = payload["FolderPath"]
    file_name = payload["FileName"]
    file_name2 = file_name
    metadata = payload["Metadata"]
    case_url = get_case_type(APIURL, session, case_id)
    request_digest = request_form_digest(APIURL, case_url, session)
    file_name = file_name.replace("'", "''")
    folder_path = folder_path.replace("'", "''")

    chunked_file_upload(APIURL, case_url, binary, file_name, session, request_digest, folder_path, orchestrator_connection)
    time.sleep(5)
    docid = get_docid(file_name2, APIURL, case_url, folder_path, session, orchestrator_connection)
    if docid is not None:
        update_metadata(APIURL, docid, session, metadata, orchestrator_connection)
        # Return the success message with DocId
        return f'{{"DocId":{docid}}}'
    else:
        return 'Failed to get DocId'