import requests
from requests_ntlm import HttpNtlmAuth
import xml.etree.ElementTree as ET
from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
import os
import json
import time

def create_ntlm_session(username: str, password: str) -> requests.Session:
    session = requests.Session()
    session.auth = HttpNtlmAuth(username, password)
    return session

def get_site_digest(site_url: str, session: requests.Session) -> str:
    """Henter et FormDigestValue for det angivne web/scope."""
    endpoint = f"{site_url}/_api/contextinfo"
    r = session.post(endpoint, headers={"Accept": "application/json; odata=verbose"})
    r.raise_for_status()
    digest = r.json()["d"]["GetContextWebInformation"]["FormDigestValue"]
    return digest

def search_sharepoint_user(root_api_url: str, session: requests.Session, digest: str, email: str):
    """Søger efter en bruger i PeoplePicker (kræver root-level digest)."""
    endpoint = f"{root_api_url}/_api/SP.UI.ApplicationPages.ClientPeoplePickerWebServiceInterface.ClientPeoplePickerSearchUser"

    headers = {
        "Accept": "application/json;odata=verbose",
        "Content-Type": "application/json;odata=verbose",
        "X-RequestDigest": digest
    }

    payload = {
        "queryParams": {
            "QueryString": email,
            "MaximumEntitySuggestions": 50,
            "AllowEmailAddresses": False,
            "AllowOnlyEmailAddresses": False,
            "PrincipalType": 1,
            "PrincipalSource": 15,
            "SharePointGroupID": 0
        }
    }

    r = session.post(endpoint, headers=headers, data=json.dumps(payload))
    r.raise_for_status()
    results = json.loads(r.json()["d"]["ClientPeoplePickerSearchUser"])
    for entity in results:
        entity_email = entity.get("EntityData", {}).get("Email")
        if entity_email and entity_email.lower() == email.lower():
            return entity
    return None

def get_list_and_id(api_url, aktid, session, aktnr):
    """Henter liste og itemid til opdatering af bruger i go."""
    endpoint = f"{api_url}/cases/{aktnr}/{aktid}/_goapi/Administration/ModernConfiguration"
    
    payload = {
        "providerTypes": ["ModernCase", "MoveDocument", "Insight", "SearchSystem", "UserSettings"]
    }
    
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    for attempt in range(5):
        r = session.post(endpoint, headers=headers, json=payload)
        if r.status_code == 200 and r.text.strip():
            break
        print(f"Tom response - venter 5 sekunder før forsøg {attempt+2}")
        time.sleep(5)
    else:
        raise Exception(f"GO svarede ikke på ModernConfiguration efter 5 forsøg for {aktid}")
    data = r.json()
    caselist = data.get("ModernCase").get("ItemServerUrl").split('/')[-2]
    itemid = data.get("ModernCase").get("ListItemID")
    return(caselist, itemid)

def update_case_field(api_url: str, session: requests.Session, digest: str, form_values: list, listnumber, item_id):
    """Opdaterer felt(er) i sagslisten."""
    endpoint = (
        f"{api_url}/aktindsigt/_api/web/GetList(@a1)/items(@a2)/ValidateUpdateListItem()"
        f"?@a1='%2Faktindsigt%2FLists%2F{listnumber}'&@a2='{item_id}'"
    )

    headers = {
        "Accept": "application/json;odata=verbose",
        "Content-Type": "application/json;odata=verbose",
        "X-RequestDigest": digest,
        "X-Sp-Requestresources": f"listUrl=%2Faktindsigt%2FLists%2F{listnumber}"
    }

    payload = {
        "formValues": form_values,
        "bNewDocumentUpdate": False,
        "checkInComment": None
    }
    try:
        r = session.post(endpoint, headers=headers, data=json.dumps(payload))
        r.raise_for_status()
        return True
    except:
        return False

def update_case_owner(api_url: str, username: str, password: str, case_id: str, email_sagsbehandler: str, orchestrator_connection, email_bruger, aktnr  ):

    """Opdaterer sagens CaseOwner-felt korrekt med to forskellige digests."""
    session = create_ntlm_session(username, password)

    # 1️⃣ Root-digest (bruges til PeoplePicker)
    root_digest = get_site_digest(api_url, session)

    # 2️⃣ Aktindsigt-digest (bruges til feltopdatering)
    akt_digest = get_site_digest(f"{api_url}/aktindsigt", session)
    
    listnumber, item_id = get_list_and_id(api_url, case_id, session, aktnr)
    # verify_case_item(api_url, session, listnumber, item_id)

    # Find bruger
    caseowner_entity = search_sharepoint_user(api_url, session, root_digest, email_sagsbehandler)
    if not caseowner_entity:
        return False
    user_entity = search_sharepoint_user(api_url, session, root_digest, email_bruger)

    form_values = [
    {
        "FieldName": "CaseOwner",
        "FieldValue": json.dumps([caseowner_entity]),
        "HasException": False,
        "ErrorMessage": None
    },
    {
        "FieldName": "SupplerendeSagsbehandlere",
        "FieldValue": json.dumps([user_entity]),
        "HasException": False,
        "ErrorMessage": None
    },
    {
        "FieldName": "CaseCategory",
        "FieldValue": "Kun sagsbehandlere på sagen",
        "HasException": False,
        "ErrorMessage": None
    },
    {
        "FieldName": "Afdeling",
        "FieldValue": "HR MTM|14a02c70-50f5-4724-97dd-7f7857930119;",
        "HasException": False,
        "ErrorMessage": None
    },
    {
        "FieldName": "KLENummer",
        "FieldValue": "81.00.00 Kommunens personale i almindelighed|daca4fc2-24d1-4776-99a2-da6d5b8ed553;",
        "HasException": False,
        "ErrorMessage": None
    },
    {
        "FieldName": "Facet",
        "FieldValue": "G01 Generelle sager|5ab851ea-f083-4ceb-9136-ec4a93f3a0e5;",
        "HasException": False,
        "ErrorMessage": None
    }
]

    # Opdater felt
    result = update_case_field(api_url, session, akt_digest, form_values, listnumber, item_id)
    return result

def verify_case_item(api_url, session, listnumber, item_id):
    endpoint = (
        f"{api_url}/aktindsigt/_api/web/GetList(@a1)/items(@a2)"
        f"?@a1='%2Faktindsigt%2FLists%2F{listnumber}'&@a2='{item_id}'"
    )
    headers = {"Accept": "application/json;odata=verbose"}
    r = session.get(endpoint, headers=headers)
    r.raise_for_status()


def close_case(case_id, session, go_api_url):
    url = f"{go_api_url}/_goapi/Cases/CloseCase"

    payload = json.dumps({
    "CaseId": case_id
    })
    headers = {
    'Content-Type': 'application/json'
    }

    response = session.post( url, headers=headers, data=payload)

    return response.text
