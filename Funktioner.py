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


def create_case(go_api_url, SagsTitel, AktID, session):
    '''
    Function for creating case in GetOrganized for the applicant to access
    '''
    url = f"{go_api_url}/geosager/_goapi/Cases"

    payload = json.dumps({
    "CaseTypePrefix": "GEO",
    "MetadataXml": f"<z:row xmlns:z=\"#RowsetSchema\" ows_Title=\"Aktindsigtssag {AktID} - {SagsTitel}\" ows_CaseStatus=\"Åben\" ows_EksterntSagsID=\"TestSagsID\" ows_EksterntSystemID=\"TestSystemID\" />",
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


def delete_case_go(go_api_url, payload, session):
    '''
    Deletes case in go
    '''
    url = f"{go_api_url}/_goapi/Cases/"
    response = session.delete(url, data=payload, timeout=1200)
    response.raise_for_status()
    return response.json()

def send_succes_email(SagsID, ModtagerMail, Url, orchestrator_connection):
    SMTP_SERVER = "smtp.adm.aarhuskommune.dk"
    SMTP_PORT = 25
    SCREENSHOT_SENDER = "PersonaleAktindsigtssag@aarhus.dk"
    subject_sagsbehandler = f"Sag nr. {SagsID}: Dokumenterne er overført til GO"


    html = f"""
    <html>
    <body>
        <p>Dokumenterne, der er angivet som 'Ja' eller 'Delvis' i dokumentlisterne er overført til GO.</p>
        <p>Du kan se sagen og gennemgå dokumenterne inden udlevering på linket herunder: </p>
        <a href = "{Url}"> Link til sagen </a> 
    </body>
    </html>
    """
    # Create the email message
    UdviklerMail = orchestrator_connection.get_constant('balas').value

    msg = EmailMessage()
    msg['To'] = ModtagerMail
    msg['From'] = SCREENSHOT_SENDER
    msg['Subject'] = subject_sagsbehandler
    msg.set_content("Please enable HTML to view this message.")
    msg.add_alternative(html, subtype='html')
    msg['Reply-To'] = UdviklerMail
    msg['Bcc'] = UdviklerMail
   
    # Send the email using SMTP
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

    try:
        folder = ctx.web.get_folder_by_server_relative_url(relative_root_folder_url)
        ctx.load(folder)
        ctx.execute_query()
        print(f"📁 Fundet mappe: {folder.properties['Name']}")
    except Exception as e:
        print("❌ Fejl ved hentning af root-mappe:", e)
        return [], []

    try:
        subfolders = folder.folders
        ctx.load(subfolders)
        ctx.execute_query()
    except Exception as e:
        print("❌ Fejl ved hentning af undermapper:", e)
        return [], []

    DokumentTitler = []
    DokIDer = []
    DokLinks = []
    aktliste_rows = []
    AktIDer = []

    for sf in subfolders:
        ctx.load(sf)
        ctx.execute_query()
        undermappe_navn = sf.properties.get("Name", "")

        try:
            files = sf.files
            ctx.load(files)
            ctx.execute_query()
        except Exception as e:
            print(f"⚠️ Kunne ikke hente filer i {undermappe_navn}:", e)
            continue

        filer_med_dato = []
        for fil in files:
            filnavn = fil.properties["Name"]
            if filnavn.endswith((".xlsx", ".xls")):
                dato = parse_dato_ddmmåååå(filnavn)
                if dato:
                    filer_med_dato.append((dato, fil))

        if not filer_med_dato:
            continue

        nyeste_fil = max(filer_med_dato, key=lambda x: x[0])[1]
        try:
            server_relative_url = nyeste_fil.properties["ServerRelativeUrl"]
            response = File.open_binary(ctx, server_relative_url)

            wb = openpyxl.load_workbook(io.BytesIO(response.content), data_only=True)
            ws = wb.active
            df = pd.read_excel(io.BytesIO(response.content), engine="openpyxl")

            doklink_kol = [c for c in df.columns if str(c) == "Link til dokument"]
            if doklink_kol:
                kol_index = df.columns.get_loc(doklink_kol[0])
                links = []
                for row in ws.iter_rows(min_row=2):
                    cell = row[kol_index]
                    if cell.hyperlink:
                        links.append(cell.hyperlink.target)
                    else:
                        links.append(cell.value)
                df[doklink_kol[0]] = links

        except Exception as e:
            print(f"⚠️ Kunne ikke læse Excel-fil: {e}")
            continue

        aktindsigt_kol = [c for c in df.columns if "Gives der aktindsigt" in c]
        dokumenttitel_kol = [c for c in df.columns if "Dokumenttitel" in c]
        dokid_kol = [c for c in df.columns if str(c) == "Dok ID"]

        # Ekstra kolonner til aktliste
        aktid_kol = [c for c in df.columns if "Akt ID" in c]
        kategori_kol = [c for c in df.columns if "Dokumentkategori" in c]
        dato_kol = [c for c in df.columns if "Dokumentdato" in c]
        bilagtil_kol = [c for c in df.columns if "Bilag til Dok ID" in c]
        bilag_kol = [c for c in df.columns if str(c) == "Bilag"]
        omfattet_kol = [c for c in df.columns if "omfattet" in str(c).lower()]
        aktindsigt_kol = [c for c in df.columns if "gives der aktindsigt" in str(c).lower()]
        begrundelse_kol = [c for c in df.columns if "Begrundelse hvis nej eller delvis" in c]

        if aktindsigt_kol and dokumenttitel_kol and dokid_kol and doklink_kol:
            kolonne = aktindsigt_kol[0]
            maske = df[kolonne].astype(str).str.lower().str.strip().str.contains("ja|delvis", na=False)
            filtreret = df[maske]

            titler = filtreret[dokumenttitel_kol[0]].dropna().tolist()
            dokider = filtreret[dokid_kol[0]].dropna().tolist()
            doklinks = filtreret[doklink_kol[0]].dropna().tolist()
            aktider = filtreret[aktid_kol[0]].dropna().tolist()

            DokumentTitler.extend(titler)
            DokIDer.extend(dokider)
            DokLinks.extend(doklinks)
            AktIDer.extend(aktider)

            for _, row in df.iterrows():  # brug hele df, ikke filtreret
                # Konverter altid til streng
                akt_id_val = "" if not aktid_kol or pd.isna(row.get(aktid_kol[0])) else str(row.get(aktid_kol[0]))
                dok_id_val = "" if not dokid_kol or pd.isna(row.get(dokid_kol[0])) else str(row.get(dokid_kol[0]))

                # Fjern evt. decimal-del hvis strengen indeholder punktum
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

                # Tilføj kun rækken hvis mindst én værdi ikke er tom
                if any(v not in [None, "", float('nan')] for v in row_dict.values()):
                    aktliste_rows.append(row_dict)
        else:
            print("⚠️ Mangler nødvendige kolonner eller tomme.")

    return list(zip(DokumentTitler, DokIDer, DokLinks, AktIDer)), aktliste_rows

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
    """
    Sletter en lokal fil ud fra stien.
    Returnerer True hvis slettet, False hvis filen ikke fandtes.
    """
    try:
        os.remove(filsti)
    except FileNotFoundError:
        print(f"Filen findes ikke: {filsti}")
    except Exception as e:
        print(f"Fejl ved sletning af {filsti}: {e}")

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
