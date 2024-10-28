import os
import re
import json
import base64
import random
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


def append_data(spreadsheet_id, creds, sheet_name, values, value_input_option='USER_ENTERED'):
    try:
        # Build the service
        service = build("sheets", "v4", credentials=creds)

        # Specify the range to append to (you can use sheet name or leave it empty)
        range_name = f'{sheet_name}!A:A'  # Append to the first column, adjust as needed

        # Specify the body of the append
        body = {
            'values': values
        }

        # Call the Sheets API to append the data
        result = service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption=value_input_option,
            insertDataOption='INSERT_ROWS',
            body=body
        ).execute()

        return result

    except Exception as e:
        print(f"An error occurred: {e}")
        return None



def get_data(spreadsheet_id, creds, range):
    try:
        service = build('sheets', 'v4', credentials=creds)

        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=spreadsheet_id,
                                    range=range).execute()
        values = result.get('values', [])
        return values  
    
    except HttpError as err:
        print("ERROR: " + str(err))


def write_data(spreadsheet_id, creds, range_name, values):
    # Build the service
    service = build('sheets', 'v4', credentials=creds)

    # Specify the body of the update
    body = {
        'values': values
    }

    # Call the Sheets API to update the sheet
    result = service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption='USER_ENTERED',
        body=body
    ).execute()

    return result


def update_range_color(spreadsheet_id, creds, range_name, color_rgb):
    from googleapiclient.discovery import build

    # Build the service
    service = build('sheets', 'v4', credentials=creds)

    # Parse the range_name
    sheet_name, cell_range = range_name.split('!')
    start_col, start_row, end_col, end_row = get_grid_range(cell_range)

    # Get the sheet ID
    sheet_id = get_sheet_id(service, spreadsheet_id, sheet_name)

    # Specify the request body for updating the background color
    requests = [
        {
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': start_row - 1,
                    'endRowIndex': end_row,
                    'startColumnIndex': start_col - 1,
                    'endColumnIndex': end_col
                },
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': {
                            'red': color_rgb[0] / 255.0,
                            'green': color_rgb[1] / 255.0,
                            'blue': color_rgb[2] / 255.0
                        }
                    }
                },
                'fields': 'userEnteredFormat.backgroundColor'
            }
        }
    ]

    # Call the Sheets API to update the sheet
    body = {'requests': requests}
    result = service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body
    ).execute()

    return result


def get_grid_range(cell_range):
    import re

    # Pattern for "A1:B2" format
    range_pattern = r'([A-Z]+)(\d+):([A-Z]+)(\d+)'
    # Pattern for single cell or full row like "A1"
    single_cell_pattern = r'([A-Z]+)(\d+)'

    range_match = re.match(range_pattern, cell_range)
    single_cell_match = re.match(single_cell_pattern, cell_range)

    if range_match:
        start_col = column_letter_to_number(range_match.group(1))
        start_row = int(range_match.group(2))
        end_col = column_letter_to_number(range_match.group(3))
        end_row = int(range_match.group(4))
        return start_col, start_row, end_col, end_row
    elif single_cell_match:
        col = column_letter_to_number(single_cell_match.group(1))
        row = int(single_cell_match.group(2))
        return col, row, None, row
    else:
        raise ValueError("Invalid cell range format")
    

def column_letter_to_number(column_letter):
    number = 0
    for char in column_letter:
        number = number * 26 + (ord(char.upper()) - ord('A') + 1)
    return number


# Initialize a global cache for sheet IDs
sheet_id_cache = {}

def get_sheet_id(service, spreadsheet_id, sheet_name):
    global sheet_id_cache

    # Create a composite key for caching
    composite_key = (spreadsheet_id, sheet_name)
    
    # Check if the sheet_id is already cached
    if composite_key in sheet_id_cache:
        return sheet_id_cache[composite_key]
    
    # If the cache is empty for this spreadsheet, populate it with all sheets
    if not any(key[0] == spreadsheet_id for key in sheet_id_cache):
        # Fetch all sheets information
        sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = sheet_metadata.get('sheets', '')
        for sheet in sheets:
            # Cache the retrieved sheet_id for each sheet
            sheet_title = sheet["properties"]["title"]
            sheet_id = sheet['properties']['sheetId']
            cache_key = (spreadsheet_id, sheet_title)
            sheet_id_cache[cache_key] = sheet_id
    
    # After populating, return the sheet_id if it exists
    if composite_key in sheet_id_cache:
        return sheet_id_cache[composite_key]
    
    raise ValueError(f"Sheet '{sheet_name}' not found in spreadsheet '{spreadsheet_id}'")


def parse_time(datetime_obj):
    local_system = os.getenv('LOCAL_SYSTEM')

    if datetime_obj.tzinfo is None:
        datetime_obj = datetime_obj.replace(tzinfo=ZoneInfo('UTC'))

    if local_system:
        datetime_obj = datetime_obj.astimezone(IST)

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

def generate_random_alphanumeric(length=12):
    """Generate a random alphanumeric string of a specified length."""
    characters = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 
                  'N', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 
                  '2', '3', '4', '5', '6', '7', '8', '9']  # Exclude O, 0, I, 1
    
    return ''.join(random.choice(characters) for _ in range(length))


def clean_phone(phone):
    phone = re.sub(r'\D', '', str(phone))
    if len(phone) > 10:
        phone = phone[-10:]
    return phone


def generate_coupon_code(coupon_code, amount, start_date, end_date=None):
    # Payload for the new price rule
    price_rule_payload = {
        "price_rule": {
            "title": coupon_code,
            "target_type": "line_item",
            "target_selection": "all",
            "allocation_method": "across",
            "value_type": "fixed_amount",
            "value": f"-{amount}",
            "customer_selection": "all",
            "starts_at": start_date,
            "usage_limit": 1,
            "entitled_product_ids": [],
            "entitled_collection_ids": [],
        }
    }

    if end_date:
        price_rule_payload['price_rule']['ends_at'] = end_date


    # Making the POST request to create the new price rule
    url_price_rule = store_url + '/price_rules.json'
    response = requests.post(url_price_rule, headers=shopify_headers, data=json.dumps(price_rule_payload))

    if response.status_code == 201:
        price_rule_data = response.json()
        price_rule_id = price_rule_data['price_rule']['id']
        
        # Now create a discount code for this price rule
        url_discount_code = store_url + f'/price_rules/{price_rule_id}/discount_codes.json'
        
        discount_code_payload = {
            "discount_code": {
                "code": coupon_code
            }
        }
        
        response_discount_code = requests.post(url_discount_code, headers=shopify_headers, data=json.dumps(discount_code_payload))
        
        if response_discount_code.status_code == 201:
            discount_code_data = response_discount_code.json()
        else:
            print("Failed to create discount code. Status code:", response_discount_code.status_code)
            print(response_discount_code.text)
            raise Exception("Failed to create discount code.")
    else:
        print("Failed to create price rule. Status code:", response.status_code)
        print(response.text)
        raise Exception("Failed to create price rule.")