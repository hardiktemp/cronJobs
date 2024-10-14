import os
from zoneinfo import ZoneInfo
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime, timedelta

from funcs import get_data, write_data, update_range_color
from funcs import generate_random_alphanumeric
from funcs import generate_coupon_code, send_message


load_dotenv()

def issue_main(creds):
    tz = ZoneInfo('Asia/Kolkata')
    
    client = MongoClient(os.getenv('MONGO_URI'))
    db = client['test']
    orders_collection = db['orders']

    customer_support_sheet_id = os.getenv('CUSTOMER_SUPPORT_AUTOMATION_ID')
    sheet_name = 'Issue Credits'

    start_date = datetime.now(tz).isoformat()
    end_date = (datetime.now(tz) + timedelta(days=180)).isoformat()

    log("\nGetting sheet data")
    sheet_data = get_data(customer_support_sheet_id, creds, sheet_name)
    header = sheet_data[0]

    already_processed = set()
    for i, row in enumerate(sheet_data[1:], start=2):
        row += [''] * (len(header) - len(row))
        order_number = row[header.index('Order Number*')]

        if not order_number:
            continue

        if (row[header.index('Processed')]):
            already_processed.add(order_number)


    log("\nProcessing each row\n")
    for i, row in enumerate(sheet_data[1:], start=2):
        row += [''] * (len(header) - len(row))
        order_number = row[header.index('Order Number*')]
        processed = row[header.index('Processed')]

        if not order_number or processed:
            continue
        
        if order_number in already_processed:
            row[header.index('Processed')] = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            row[header.index('Message Sent')] = "Y"
            row[header.index('Remarks')] += "Already processed earlier."
            write_data(customer_support_sheet_id, creds, f'{sheet_name}!A{i}', [row])
            continue

        amount = row[header.index('Credit Amount*')]

        if not amount:
            row[header.index('Remarks')] = "Amount not provided"
            color_and_update(header, row, i, customer_support_sheet_id, creds, sheet_name)
            continue

        print(f"Processing row {i} - #{order_number}")
        order = orders_collection.find_one({'order_number': int(order_number)})
        if not order:
            row[header.index('Remarks')] = "Order not found"
            color_and_update(header, row, i, customer_support_sheet_id, creds, sheet_name)
            continue

        phone = order['phone']
        if not phone:
            row[header.index('Remarks')] = "Phone not found"
            color_and_update(header, row, i, customer_support_sheet_id, creds, sheet_name)
            continue

        coupon_code = generate_random_alphanumeric(12)

        generate_coupon_code(coupon_code, amount, start_date, end_date)
        success = send_message(phone, 'credits', {"credit_amount": amount, "coupon_code": coupon_code})

        row[header.index('Processed')] = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
        row[header.index('Phone')] = phone
        row[header.index('Coupon Code')] = coupon_code
        row[header.index('Message Sent')] = "Y" if success else "N"
        row[header.index('Remarks')] = ""

        write_data(customer_support_sheet_id, creds, f'{sheet_name}!A{i}', [row])
        already_processed.add(order_number)


def color_and_update(header, row, index, sheet_id, creds, sheet_name):
    red_rgb = (234,153,153)
    end_col = chr(ord('A') + len(header) - 1) 
    update_range_color(sheet_id, creds, f'{sheet_name}!A{index}:{end_col}{index}', red_rgb)
    write_data(sheet_id, creds, f'{sheet_name}!A{index}', [row])

 

def log(message, indent=0):
    print(f"{' ' * (4 * indent)}{message}")




