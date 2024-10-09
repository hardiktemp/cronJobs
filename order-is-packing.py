import os

from zoneinfo import ZoneInfo
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient

from funcs import authenticate, get_data, append_data, send_message, parse_time

load_dotenv()

def main():
    client = MongoClient(os.getenv('MONGO_URI'))
    db = client['test']
    orders_collection = db['orders']

    creds = authenticate()
    sheet_name = 'Order-is-packing'
    log_sheet_id = os.getenv('LOG_SPREADSHEET_ID')
    work_sheet_id = os.getenv('WORK_SPREADSHEET_ID')

    work_sheet_data = get_data(creds, work_sheet_id, 'Sheet1')
    header = work_sheet_data[0]
    work_sheet_work_set  =set()
    for row in work_sheet_data[1:]:
        work_sheet_work_set.add(row[header.index('Work')])
    

    processed_orders = set()
    sheet_data = get_data(creds, log_sheet_id, sheet_name)
    header = sheet_data[0]
    for row in sheet_data[1:]:
        order_number = row[header.index('Order Number')]
        processed_orders.add(order_number)

    query = {
        'cancelled': False,
        'fullfilment_status': {'$ne': 'fulfilled'},
        'financial_status': {'$nin': ['refunded', 'voided']},
    }
    orders = orders_collection.find(query)

    now = parse_time(datetime.now())
    curr_datetime = now.strftime('%d-%m-%Y %H:%M:%S')
    
    local_system = os.getenv('LOCAL_SYSTEM')
    if local_system == False:
        curr_datetime_ist = now.astimezone(ZoneInfo(IST)).strftime('%d-%m-%Y %H:%M:%S')
    else:
        curr_datetime_ist = curr_datetime

    for order in orders:
        created_at = parse_time(order['created_at'].replace(tzinfo=ZoneInfo('UTC')))
        order_number = str(int(order['order_number']))
        
         
        
        if (now - created_at).days >= 4:
            msg = f'Order {order_number} is not packed yet.'
            if msg in work_sheet_work_set:
                continue
            row =  [curr_datetime_ist, msg, ""]
            append_data(creds, work_sheet_id, "Sheet1", [row])

        elif (now - created_at).days >= 2:
            if order_number in processed_orders:
                continue

            phone = str(order['phone'])  
            for _ in range(3):
                success = send_message(phone, 'order_is_packing')
                if success:
                    row = [curr_datetime_ist, order_number, phone, 'Success']
                    append_data(creds, log_sheet_id, sheet_name, [row])
                    break
            else:
                row = [curr_datetime_ist, order_number, phone, 'Failed']
                append_data(creds, log_sheet_id, sheet_name, [row])
                
    

if __name__ == '__main__':
    IST = ZoneInfo('Asia/Kolkata')
    main()