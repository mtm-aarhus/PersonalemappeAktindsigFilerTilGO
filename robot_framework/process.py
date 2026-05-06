from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement
import os
from datetime import datetime
import json
import time
from OpretAktliste import invoke_GenerateAndUploadAktlistePDF
from Funktioner import *
from sqlalchemy import create_engine, text
from datetime import datetime
from urllib.parse import quote_plus, quote
import smtplib
from email.message import EmailMessage
from robot_framework import config
from GoBrugerstyring import *

def send_error_email(to_address: str , caseid):
    """Sends and email to caseworker if caseurl is not valid (most likely invalid casenumber)
    """
    # Create message
    msg = EmailMessage()
    msg['to'] = to_address
    msg['from'] = "personaleindsigt@aarhus.dk"
    msg['subject'] = f"Fejl! Filer ikke overført: {caseid}"

    # Create an HTML message with the exception and screenshot
    html_message = f"""
    <html>
        <body>
            <p>Du mangler at oprette en dokumentliste for {caseid}</p>
            <p>Tryk 'accepter' for at oprette dokumentlisten.</p>
        </body>
    </html>
    """

    msg.set_content("Please enable HTML to view this message.")
    msg.add_alternative(html_message, subtype='html')

    # Send message
    with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as smtp:
        smtp.send_message(msg)

def process(orchestrator_connection: OrchestratorConnection, queue_element: QueueElement | None = None) -> None:
    specific_content = json.loads(queue_element.data)
    # specific_content = json.loads(queue_element) #Til test

    SharepointSiteUrl = orchestrator_connection.get_constant("AktindsigtPersonalemapperSharepointURL").value
    go_api_url = orchestrator_connection.get_constant("GOApiURL").value
    go_api_login = orchestrator_connection.get_credential("GOAktApiUser")
    robot_user = orchestrator_connection.get_credential("Robot365User")
    username = robot_user.username
    password = robot_user.password
    go_username = go_api_login.username
    go_password = go_api_login.password

    orchestrator_connection.log_info('Got constants')

    #Definer variable
    SagsID = specific_content.get('caseid')
    SagsbehandlerMail = specific_content.get('SagsbehandlerEmail')
    AnmoderMail = specific_content.get('AnmoderMail')
    PersonaleSagsTitel= specific_content.get('PersonaleSagsTitel')
    Udleveringsmappelink = specific_content.get('Udleveringsmappelink')
    dokumentlisteovermappe = specific_content.get("dokumentlisteovermappe")
    if not dokumentlisteovermappe:
        send_error_email(SagsbehandlerMail, SagsID)
        return

    orchestrator_connection.log_info(f'Variable {SagsID}, {PersonaleSagsTitel}')
    session = create_session(go_username, go_password)

    if Udleveringsmappelink:
        #hvis der allerede ligger en udleveringsmappe skal den slettes for ikke at have dobbeltmapper til at ligge
        UdleveringsSagsID = Udleveringsmappelink.rsplit("/")[-1]
        orchestrator_connection.log_info(f'Gammel udleveringsmappe detekteret {UdleveringsSagsID} {Udleveringsmappelink}')
        delete_case_go(go_api_url, session, UdleveringsSagsID)
        orchestrator_connection.log_info(f'Gammel delingsmappe slettet for sag {UdleveringsSagsID}')
    else:
        orchestrator_connection.log_info(f'Ingen udleveringsmappe i forvejen {Udleveringsmappelink}')
    #1 - definer sharepointsite url og mapper
    orchestrator_connection.log_info('Defining sharepoint stuff')

    relative_url = f'/{SharepointSiteUrl.split(".com/")[-1]}/Delte dokumenter/Dokumentlister/{dokumentlisteovermappe}'


    downloads_folder = os.path.join(os.path.expanduser("~"), "Downloads")
    today_date = datetime.now().strftime("%d-%m-%Y")

    #2 - Hent dokumenttitlerne der er ja eller delvis i i dokumentlisterne og download filerne, hvis der er nogen
    res, aktliste_data = hent_dokumenttitler_nyeste_filer(SharepointSiteUrl, relative_url, username, password, orchestrator_connection= orchestrator_connection)
    orchestrator_connection.log_info('Dokumentliste tjekket')

    #3 - Opret en sag
    orchestrator_connection.log_info('Opretter sag')
    session = create_session(go_username, go_password)
    CreatedCase = json.loads(create_case(go_api_url, PersonaleSagsTitel, SagsID, session))
    
    RelativeSagsUrl = CreatedCase['CaseRelativeUrl']
    CaseUrl = f'{go_api_url}/{RelativeSagsUrl}'
    CaseID = CreatedCase['CaseID']

    #Setting caseworker first so no documents are visible when uploaded
    mailHR = orchestrator_connection.get_constant('jadt').value #ændres i prod
    result = update_case_owner(api_url= go_api_url, username= go_username, password= go_password, case_id= CaseID, email_sagsbehandler= SagsbehandlerMail, orchestrator_connection= orchestrator_connection, email_bruger= mailHR )
    if not result:
        orchestrator_connection.log_error('Bruger kan ikke fremsøges i GO')
        raise Exception
    else:
        print('Caseworker updated succesfully')
    
    #og upload filerne hvis der er nogen
    orchestrator_connection.log_info('Uploader filer')
    ikke_konverterede_filer = []  # Tilføj før loopet
    fejlede_uploads = []
    created_folders = set()

    for file in res:
        orchestrator_connection.log_info('Processing new file')
        FilEndelse = file[2].rsplit('.')[-1]
        UnderMappeNavn = file[4]
        file_path = f'{downloads_folder}\{file[0]}.{FilEndelse}'
        AktID = file[3]

        byte_result, is_pdf, ikke_konverteret = try_convert_go_file_to_pdf(
            go_api_url, file[1], session, go_username, go_password, go_api_url, file_path, orchestrator_connection
        )

        if ikke_konverteret:
            ikke_konverterede_filer.append(ikke_konverteret)

        if byte_result is None:
            orchestrator_connection.log_info(f"Kunne ikke hente fil {file[0]} - springer over")
            fejlede_uploads.append(file[0])
            continue

        if is_pdf:
            FilEndelse = "pdf"

        filename = f'{AktID} - {file[0]}.{FilEndelse}'
        byte_arr = list(byte_result)

        ows_dict = {
            "Title": filename,
            "CaseID": CaseID,
            "Beskrivelse": "Uploaded af personaleindsigt",
            "Korrespondance": "Udgående",
            "Dato": today_date,
            "CCMMustBeOnPostList": "0"
        }
        payload = make_payload_document(
            ows_dict=ows_dict,
            caseID=CaseID,
            FolderPath=UnderMappeNavn,
            byte_arr=byte_arr,
            filename=filename
        )

        try:
            if (len(byte_result) / (1024 * 1024)) > 10:
                raise Exception("Fil er større end 10 MB, forsøger chunk-upload")
            response = upload_document_go(go_api_url, payload=payload, session=session)
            if "DocId" not in response:
                raise Exception("No DocId i response")

        except Exception as e:
            orchestrator_connection.log_info(f"Normal upload fejlede for {filename}: {e}")
            uploaded = False
            max_retries = 3
            for attempt in range(1, max_retries + 1):
                try:
                    orchestrator_connection.log_info(f"Chunk-upload forsøg {attempt} for {filename}")
                    if UnderMappeNavn and UnderMappeNavn not in created_folders:
                        create_and_delete_placeholder(
                            go_api_url, CaseID, UnderMappeNavn, session, orchestrator_connection
                        )
                        created_folders.add(UnderMappeNavn)

                    large_response = upload_large_document(
                        go_api_url, payload, session, byte_result, orchestrator_connection
                    )
                    large_response_json = json.loads(large_response)
                    if "DocId" not in large_response_json:
                        raise Exception(f"Ingen DocId i chunk-response for {filename}")
                    uploaded = True
                    break
                except Exception as retry_exception:
                    orchestrator_connection.log_info(f"Chunk-upload forsøg {attempt} fejlede: {retry_exception}")
                    if attempt == max_retries:
                        orchestrator_connection.log_info(f"Alle upload-metoder fejlede for {filename}")
                        fejlede_uploads.append(filename)

        delete_local_file(filsti=file_path)
    args = {
    "in_dt_AktIndex": aktliste_data,
    "in_Sagsnummer": PersonaleSagsTitel,
    "in_DokumentlisteDatoString": today_date,
    "in_GoUsername": go_username,
    "in_GoPassword": go_password,
    "in_CaseID": CaseID,}
    orchestrator_connection.log_info('Making aktliste')
    if aktliste_data:
        invoke_GenerateAndUploadAktlistePDF(args, session=session, gourl=go_api_url)
    else:
        orchestrator_connection.log_info('Aktliste er tom - springer generering over')
        send_ingen_doko_mail(SagsID= SagsID, ModtagerMail= SagsbehandlerMail, orchestrator_connection= orchestrator_connection)
    orchestrator_connection.log_info('Setting case owner')
    CaseUrlUser = CaseUrl.replace("ad.", "", 1)
    send_succes_email(SagsID=SagsID,ModtagerMail=SagsbehandlerMail,Url=CaseUrlUser,orchestrator_connection=orchestrator_connection,ikke_konverterede_filer=ikke_konverterede_filer,fejlede_uploads=fejlede_uploads)
    orchestrator_connection.log_info('Logging info to database')
    SQL_SERVER = orchestrator_connection.get_constant('SqlServer').value 
    DATABASE_NAME = "AktindsigterPersonalemapper"

    odbc_str = (
        "DRIVER={SQL Server};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={DATABASE_NAME};"
        "Trusted_Connection=yes;"
    )

    odbc_str_quoted = quote_plus(odbc_str)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={odbc_str_quoted}", future=True)

    sql = text("""
        UPDATE dbo.cases
        SET Udleveringsmappelink = :link,
            last_run_transfer_go = :ts
        WHERE aktid = :caseid
    """)

    with engine.begin() as conn:
        result = conn.execute(sql, {
            "link": CaseUrlUser,
            "ts": datetime.now(),
            "caseid": str(SagsID)
        })
        if result.rowcount == 0:
            orchestrator_connection.log_info(f"⚠️ Ingen sag fundet med aktid={SagsID}")
        else:
            orchestrator_connection.log_info(f"✅ Opdateret sag {SagsID} med udleveringslink:")
