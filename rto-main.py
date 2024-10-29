import os
import json
import requests
import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from pymongo import MongoClient

from funcs import store_url, shopify_headers
from funcs import authenticate, get_data, write_data
from funcs import generate_coupon_code, send_message
from funcs import generate_random_alphanumeric, clean_phone

load_dotenv()

RTO_SPREADSHEET_ID = os.getenv('RTO_SPREADSHEET_ID')

def main():
    creds = authenticate()

    client = MongoClient(os.getenv('MONGO_URI'))
    db = client['test']
    orders_collection = db['orders']

    print("\nProcessing CoD RTOs\n")
    process_cod(creds, orders_collection)

    print("\nProcessing Prepaid RTOs\n")
    process_prepaid(creds)
    

def process_prepaid(creds):
    tz = ZoneInfo('Asia/Kolkata')
    sheet_data = get_data(RTO_SPREADSHEET_ID, creds, 'Prepaid')
    header = sheet_data[0]

    for i, row in enumerate(sheet_data[1:], start=2):
        if not row:
            continue

        row += [''] * (len(header) - len(row))
        if row[header.index('Resolution')].lower().strip() != 'coupon':
            continue

        if not row[header.index('Scan Date')] and row[header.index('Manual Received')].lower().strip() != 'y':
            continue

        if row[header.index('Coupon Code')]:
            continue

        order_number = row[header.index('Order No')]
        print(f"\nProcessing row {i}")
        amount = float(row[header.index('Amount')].replace(',', '').strip())
        phone = clean_phone(row[header.index('Phone')])
        
        if not phone:
            print(f"    Error: No phone found for order {order_number}")
            continue
        
        coupon_code = generate_random_alphanumeric(12)
        start_date = datetime.datetime.now(tz).isoformat()
        end_date = (datetime.datetime.now(tz) + datetime.timedelta(days=180)).isoformat()
        generate_coupon_code(coupon_code, amount, start_date, end_date)

        print(f"    Sending message to {phone}")
        success = send_message(phone, 'return_credits_4', {'credit_amount': amount, 'coupon_code': coupon_code})
        coupon_code_column = chr(ord('A') + header.index('Coupon Code'))
        print(f"    Writing code {coupon_code} to sheet")
        write_data(RTO_SPREADSHEET_ID, creds, f'Prepaid!{coupon_code_column}{i}', [[coupon_code]])

        conveyed_column = chr(ord('A') + header.index('Conveyed to Customer'))
        conveyed = 'Y' if success else 'N'
        write_data(RTO_SPREADSHEET_ID, creds, f'Prepaid!{conveyed_column}{i}', [[conveyed]])



def process_cod(creds, orders_collection):
    sheet_data = get_data(RTO_SPREADSHEET_ID, creds, 'CoD')
    header = sheet_data[0]
    for i, row in enumerate(sheet_data[1:], start=2):
        if not row:
            continue
        
        order_number = row[header.index('Order No')]
        if order_number == '':
            continue

        # make all rows equal length
        row.extend([''] * (len(header) - len(row)))

        if not (row[header.index('Process Date')]):
            print(f"Processing row - {i}")
            order = orders_collection.find_one({'order_number': int(order_number)})
            if order is None:
                print('\n !!! Order not found:', order_number)
                continue

    
            phone = clean_phone(order['phone'])
            first_name = order['first_name']
            last_name = order['last_name']
            full_name = first_name + ' ' + last_name
            financial_status = order['financial_status']

            row[header.index('Process Date')] = datetime.datetime.now().strftime("%d/%m/%Y")
            row[header.index('Order Amount')] = order['price']
            row[header.index('Person')] = full_name
            row[header.index('Phone Number')] = phone
            if financial_status == 'paid':
                print(f"Order {order_number} is already paid. Skipping.")
                row[header.index('Remarks')] = "Paid Order"
            else:
                print(f"\nCancelling order: {order_number}")
                msg = cancel_order(order)
                row[header.index('Remarks')] = msg
                print(f"    Sending message to {phone}")
                success = send_message(phone, 'rto_warning')
                row[header.index('Message Sent')] = 'Yes' if success else 'No'
            write_data(RTO_SPREADSHEET_ID, creds, f'CoD!A{i}', [row])


def cancel_order(order):
    order_id = order['id']

    # mark transaction as voided
    url = store_url + f"orders/{order_id}/transactions.json"
    total_amount = order['price']
    payload = {
        "transaction": {
            "currency": "INR",
            "amount": total_amount,
            "kind": "void",
        }
    }
    response = requests.post(url, headers=shopify_headers, data=json.dumps(payload))
    if response.status_code != 201:
        print(response.text)
        print(f"\nOrder Num: {order['order_number']}")
        return "Cannot void"

    # cancel the order
    url = store_url + f"orders/{order_id}/cancel.json"
    payload = {
        "reason": "declined",
    }
    response = requests.post(url, headers=shopify_headers, data=json.dumps(payload))
    if response.status_code != 200:
        print(response.text)
        print(f"\nOrder Num: {order['order_number']}")
        return "Cannot Cancel"


if __name__ == '__main__':
    main()