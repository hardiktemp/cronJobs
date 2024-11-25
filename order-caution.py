import os

from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime, timedelta

from funcs import authenticate, append_data, send_message

load_dotenv()

def main():
    client = MongoClient(os.getenv('MONGO_URI'))
    db = client['test']
    orders_collection = db['orders']

    creds = authenticate()
    sheet_name = 'order-caution'
    log_sheet_id = os.getenv('LOG_SPREADSHEET_ID')


    today = datetime.now(IST).replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = today + timedelta(days=1)
    query = {
        'cancelled': False,
        'fullfilment_status': 'fulfilled',
        'fulfillments': {
            '$elemMatch': {
                'created_at': {'$gte': today.isoformat(), '$lt': tomorrow.isoformat()}
            }
        }
    }

    now = datetime.now(IST)
    curr_datetime_ist = now.strftime('%d-%m-%Y %H:%M:%S')
    
    for order in orders_collection.find(query):
        order_number = order['order_number']
        phone = str(order['phone'])
        for _ in range(3):
            success = send_message(phone, 'order_caution')
            if success:
                row = [curr_datetime_ist, order_number, phone, 'Success']
                append_data(log_sheet_id, creds, sheet_name, [row])
                break
        else:
            row = [curr_datetime_ist, order_number, phone, 'Failed']
            append_data(log_sheet_id, creds, sheet_name, [row])


if __name__ == '__main__':
    IST = ZoneInfo('Asia/Kolkata')
    main()
