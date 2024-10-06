import os
import json
import base64
import requests
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials

load_dotenv()

IST = ZoneInfo('Asia/Kolkata')
shopify_key = os.getenv('SHOPIFY_API_KEY')
store_url = f'https://{shopify_key}/admin/api/2024-04/'

shopify_headers = {
    'X-Shopify-Access-Token': shopify_key.split(':')[1].split('@')[0],
    'Content-Type': 'application/json'
}

def authenticate():
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    encoded_credentials = os.getenv('GOOGLE_CREDENTIALS_BASE64')
    credentials_json = base64.b64decode(encoded_credentials).decode('utf-8')
    credentials_dict = json.loads(credentials_json)
    creds = Credentials.from_service_account_info(credentials_dict, scopes=scopes)
    return creds


def get_last_row(service, spreadsheet_id, sheet_name):
    # Retrieve the data from the sheet to find the last row
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f'{sheet_name}'
    ).execute()

    values = result.get('values', [])
    return len(values) + 1


def append_data(creds, spreadsheet_id, sheet_name, values, value_input_option='USER_ENTERED'):
    try:
        # Build the service
        service = build("sheets", "v4", credentials=creds)

        # Find the next available row
        next_row = get_last_row(service, spreadsheet_id, sheet_name)
        range_name = f'{sheet_name}!A{next_row}'

        # Specify the body of the append
        body = {
            'values': values
        }

        # Call the Sheets API to append the data
        result = (
            service.spreadsheets()
            .values()
            .update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption=value_input_option,
                body=body
            )
            .execute()
        )

        return result

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def get_data(creds, spreadsheet_id, range):
    try:
        service = build('sheets', 'v4', credentials=creds)

        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=spreadsheet_id,
                                    range=range).execute()
        values = result.get('values', [])
        return values  
    
    except HttpError as err:
        print("ERROR: " + str(err))


def parse_time(datetime_obj):
    local_system = os.getenv('LOCAL_SYSTEM')

    if datetime_obj.tzinfo is None:
        datetime_obj = datetime_obj.replace(tzinfo=ZoneInfo('UTC'))

    if local_system:
        datetime_obj = datetime_obj.astimezone(ZoneInfo(IST))

    return datetime_obj


def send_message(phone, template_name, params_dict={}):
    WATI_TOKEN = os.getenv('WATI_TOKEN')
    WATI_CLIENT_ID = os.getenv('WATI_CLIENT_ID')

    # add the user first
    headers = {
        "content-type": "text/json",
        "Authorization": WATI_TOKEN
    }
   

    url = f"https://live-mt-server.wati.io/{WATI_CLIENT_ID}/api/v1/addContact/91{phone}"
    payload = {
        "customParams": [
            {
                "name": "dummy",
                "value": "dummy"
            }
        ],
    }

    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code != 200 or response.json().get('result') == False:
        return False

    # send the message
    url = f"https://live-mt-server.wati.io/{WATI_CLIENT_ID}/api/v1/sendTemplateMessage?whatsappNumber=91{phone}"

    parameters_list = [
        {
            "name": key,
            "value": str(value) if value is not None else None
        }
        for key, value in params_dict.items()
    ]

    payload = {
        "parameters": parameters_list,
        "broadcast_name": f'{template_name}_broadcast',
        "template_name": template_name
    }

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code != 200 or response.json().get('result') == False:
        return False
    
    return True
