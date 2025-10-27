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

def process(orchestrator_connection: OrchestratorConnection, queue_element: QueueElement | None = None) -> None:
    specific_content = json.loads(queue_element.data)

    SharepointSiteUrl = orchestrator_connection.get_constant("AktindsigtPersonalemapperSharepointURL").value
    gotesturl = orchestrator_connection.get_constant('GOApiTESTURL').value
    go_api_url = orchestrator_connection.get_constant("GOApiURL").value
    go_api_login = orchestrator_connection.get_credential("GOAktApiUser")
    robot_user = orchestrator_connection.get_credential("Robot365User")
    username = robot_user.username
    password = robot_user.password
    go_username = go_api_login.username
    go_password = go_api_login.password
    go_test_login = orchestrator_connection.get_credential("GOTestApiUser")
    go_username_test = go_test_login.username
    go_password_test = go_test_login.password

    specific_content = json.loads(queue_element.data)
    orchestrator_connection.log_info('Got constants')

    #Definer variable
    SagsID = specific_content.get('caseid')
    SagsbehandlerMail = specific_content.get('SagsbehandlerEmail')
    PersonaleSagsTitel= specific_content.get('PersonaleSagsTitel')
    Udleveringsmappelink = specific_content.get('Udleveringsmappelink')
    dokumentlisteovermappe = specific_content.get("dokumentlisteovermappe")

    orchestrator_connection.log_info(f'Variable {SagsID}, {PersonaleSagsTitel}')

    if Udleveringsmappelink:
        #hvis der allerede ligger en udleveringsmappe skal den slettes for ikke at have dobbeltmapper til at ligge
        UdleveringsSagsID = Udleveringsmappelink.rsplit("/")[-1]
        orchestrator_connection.log_info(f'Gammel udleveringsmappe detekteret {UdleveringsSagsID} {Udleveringsmappelink}')
        # delete_case_go(gotesturl, UdleveringsSagsID, session)
        # orchestrator_connection.log_info(f'Gammel delingsmappe slettet for sag {UdleveringsSagsID}')
    #1 - definer sharepointsite url og mapper
    orchestrator_connection.log_info('Defining sharepoint stuff')

    relative_url = f'/{SharepointSiteUrl.split(".com/")[-1]}/Delte dokumenter/Dokumentlister/{dokumentlisteovermappe}'
    print(relative_url)

    downloads_folder = os.path.join(os.path.expanduser("~"), "Downloads")
    today_date = datetime.now().strftime("%d-%m-%Y")

    #2 - Hent dokumenttitlerne der er ja eller delvis i i dokumentlisterne og download filerne, hvis der er nogen
    res, aktliste_data = hent_dokumenttitler_nyeste_filer(SharepointSiteUrl, relative_url, username, password, orchestrator_connection= orchestrator_connection)
    orchestrator_connection.log_info('Dokumentliste tjekket')

    #3 - Opret en sag
    orchestrator_connection.log_info('Opretter sag')
    session = create_session(go_username_test, go_password_test)
    CreatedCase = json.loads(create_case(gotesturl, PersonaleSagsTitel, SagsID, session))
    
    RelativeSagsUrl = CreatedCase['CaseRelativeUrl']
    CaseUrl = f'{gotesturl}/{RelativeSagsUrl}'
    CaseID = CreatedCase['CaseID']
    
    #og upload filerne hvis der er nogen
    orchestrator_connection.log_info('Uploader filer')
    if res:
        for file in res:
            orchestrator_connection.log_info('Processing new file')
            FilEndelse = file[2].rsplit('.')[-1]
            file_path = f'{downloads_folder}\{file[0]}.{FilEndelse}'
            AktID = file[3]
            filename = f'{AktID} - {file[0]}.{FilEndelse}'
            
            download_file(file_path, file[1], go_api_url, go_username, go_password)
            time.sleep(3)

            with open(file_path, "rb") as local_file:
                file_content = local_file.read()
                byte_arr = list(file_content)

            ows_dict = {
                        "Title": filename,
                        "CaseID": CaseID,  # Replace with your case ID
                        "Beskrivelse": "Uploaded af personaleaktbob",  # Add relevant description
                        "Korrespondance": "Udgående",
                        "Dato": today_date,
                        "CCMMustBeOnPostList": "0"
                    }
            orchestrator_connection.log_info('Making payload doc')
            payload = make_payload_document(ows_dict= ows_dict, caseID = CaseID, FolderPath= "", byte_arr= byte_arr, filename = filename )
            orchestrator_connection.log_info('uploading docs')

            upload_document_go(gotesturl, payload = payload, session = session)
            delete_local_file(filsti = file_path)
        args = {
        "in_dt_AktIndex": aktliste_data,
        "in_Sagsnummer": CaseID,
        "CasePath": RelativeSagsUrl, 
        "in_DokumentlisteDatoString": today_date,
        "in_GoUsername": go_username_test,
        "in_GoPassword": go_password_test}
        orchestrator_connection.log_info('Makinf aktliste')
        invoke_GenerateAndUploadAktlistePDF(args, orchestrator_connection= orchestrator_connection, session = session, gourl = gotesturl)
        send_succes_email(SagsID= SagsID, ModtagerMail= SagsbehandlerMail, Url = CaseUrl, orchestrator_connection = orchestrator_connection)
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
                "link": CaseUrl,
                "ts": datetime.now(),
                "caseid": str(SagsID)
            })
            if result.rowcount == 0:
                orchestrator_connection.log_info(f"⚠️ Ingen sag fundet med aktid={SagsID}")
            else:
                orchestrator_connection.log_info(f"✅ Opdateret sag {SagsID} med udleveringslink:")