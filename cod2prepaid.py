import os
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from pymongo import MongoClient

from funcs import store_url, shopify_headers, append_data, authenticate

'''
This code checks all the orders that are CoD < Rs.20 and unfullfilled and marks them as paid.
'''

load_dotenv()

def main():
    client = MongoClient(os.getenv('MONGO_URI'))
    db = client['test']
    orders_collection = db['orders']

    query = {
        'cancelled': False,
        'financial_status': {'$in': ['pending', 'partially_paid']},
        'fullfilment_status': None,
        'price': {'$lte': 20}
    }

    orders = orders_collection.find(query)
    
    curr_datetime_ist = datetime.now(IST).strftime('%d-%m-%Y %H:%M:%S')

    for order in orders:
        order_id = order['id']
        order_num = order['order_number']
        amount = order['price']
        log(f"Processing #{order_num} - {order_id}", indent=0)
        for _ in range(3):
            success = mark_as_paid(order_id)
            if success:
                row = [curr_datetime_ist, order_num, amount, 'True']
                append_data(creds, log_sheet_id, sheet_name, [row])
                break
        else:
            row = [curr_datetime_ist, order_num, amount, 'False']
            append_data(creds, log_sheet_id, sheet_name, [row])


def mark_as_paid(order_id):
    url = store_url + f'orders/{order_id}/transactions.json'
    
    payload = {
        'transaction': {
            'source': 'external',
            'kind': 'capture',
            'status': 'success',
            'gateway': 'Cash on Delivery'
        }
    }

    
    response = requests.post(url, json=payload, headers=shopify_headers)

    if response.status_code == 201:
        log("Successfully marked", indent=1)
        return True
    else:
        log("E: Failed to mark as paid", indent=1)
        return False
    
def log(msg, indent=0):
    print('  ' * indent + msg)

if __name__ == "__main__":
    IST = ZoneInfo('Asia/Kolkata')
    creds = authenticate()
    log_sheet_id = os.getenv('LOG_SPREADSHEET_ID')
    sheet_name = 'cod2prepaid'
    main()