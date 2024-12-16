import os
import datetime
import requests
import pandas as pd
from typing import Tuple
from dotenv import load_dotenv
from pymongo import MongoClient

from funcs import authenticate, get_data, write_data, update_range_color
from funcs import store_url, shopify_headers
from funcs import send_message, clean_phone

load_dotenv()

'''
Send message has been updated. Check if it works.

This code is ready but not tested rigrously.
Please use with caution.

'''

def main(creds):
    raise Exception("Have to check message sending")
    sheet_name = 'Merge Credits'
    sheet_id = os.getenv('CUSTOMER_SUPPORT_AUTOMATION_ID')
    client = MongoClient(os.getenv('MONGO_URI'))
    db = client['test']
    orders_collection = db['orders']

    log("\nGetting sheet data")
    sheet_data = get_data(sheet_id, creds, sheet_name)
    header = sheet_data[0]

    for i, row in enumerate(sheet_data[1:], start=2):
        log("\nProcessing each row\n")
        row += [''] * (len(header) - len(row))
        coupon_codes = row[header.index('Coupon Codes*')].split(',')
        phone_number = clean_phone(row[header.index('Phone Number*')])
        
        if not coupon_codes:
            continue

        if len(coupon_codes) < 2:
            row[header.index('Remarks')] = 'Multiple coupons not present'
            color_and_update(header, row, i, sheet_id, sheet_name)
            continue

        # clean the codes
        coupon_codes = [code.strip() for code in coupon_codes]
        
        if not phone_number:
            row[header.index('Error')] = 'Phone number not present'
            color_and_update(header, row, i, sheet_id, sheet_name)
            continue

        
        if row[header.index('Processed')]:
            continue

        log(f"Processing row {i}")
        valid_coupons = []
        for coupon_code in coupon_codes:
            is_valid, coupon, reason, tech_error = check_coupon(coupon_code)
            if tech_error:
                row[header.index('Remarks')] += f'Tech Error - {coupon_code}\n'
                break
            
            if reason == 'Cannot find coupon':
                row[header.index('Remarks')] += f'Cannot find coupon - {coupon_code}\n'
                color_and_update(header, row, i, sheet_id, sheet_name)
                break

            if not is_valid:
                row[header.index('Remarks')] += f'{coupon_code} - {reason}\n'
                continue

            # find all orders from the db on which this coupon has been used
            orders = orders_collection.find({"discount_codes": {
                                                "$elemMatch": {"code": coupon_code}
                                            }
                                        })

            amount_used, usage_cnt = 0, 0
            for order in orders:
                if order['cancelled']:
                    continue

                for discount in order['discount_codes']:
                    if discount['code'] == coupon_code:
                        amount_used += float(discount['amount'])
                        usage_cnt += 1

            if coupon.usage_count < coupon.usage_limit:
                    valid_coupons.append(coupon)
                    continue
            
            elif coupon.usage_count > coupon.usage_limit:
                row[header.index('Remarks')] += f'Usage > Limit, please check manually {coupon_code}\n'
                color_and_update(header, row, i, sheet_id, sheet_name)
                continue
            
            # renewing the coupon if applicable
            else: # coupon.usage_count == coupon.usage_limit:
                new_balance = coupon.amount - amount_used
                if new_balance > 1:
                    if not update_coupon(coupon, new_balance):
                        row[header.index('Remarks')] += f'Tech Error Updating {coupon_code}\n'
                        continue
                    valid_coupons.append(coupon)
                else: 
                    row[header.index('Remarks')] += f'Coupon {coupon_code} already used\n'

        # Merge here
        num_valid_coupons = len(valid_coupons)
        if num_valid_coupons > 1:
            amount = 0
            old_coupon_codes = []
            for j in range(1, num_valid_coupons):
                amount += valid_coupons[j].amount
                old_coupon_codes.append(valid_coupons[j].code)
                for _ in range(3):
                    if disable_coupon(valid_coupons[j]):
                        break
            
            amount += valid_coupons[0].amount
            success = update_coupon(valid_coupons[0], amount, valid_coupons[0].usage_limit)
            if success:
                row[header.index('Processed')] = datetime.datetime.now().strftime('%d-%m-%Y %H:%M')
                row[header.index('Updated Code')] = valid_coupons[0].code
                row[header.index('Updated Amount')] = amount
                
                success = send_message(phone_number, 'credits_merged_1', {"new_coupon_code": valid_coupons[0].code, "new_credit_amount": amount, "old_coupon_codes": ", ".join(old_coupon_codes)})

                row[header.index('Message Sent')] = 'Yes' if success else 'No'
                write_data(sheet_id, creds, f'{sheet_name}!A{i}', [row])

        elif num_valid_coupons == 1:
            row[header.index('Processed')] = datetime.datetime.now().strftime('%d-%m-%Y %H:%M')
            row[header.index('Remarks')] += 'Only one valid coupon\n'
            write_data(sheet_id, creds, f'{sheet_name}!A{i}', [row])
        else:
            row[header.index('Processed')] = datetime.datetime.now().strftime('%d-%m-%Y %H:%M')
            row[header.index('Remarks')] += 'No valid coupons found\n'
            write_data(sheet_id, creds, f'{sheet_name}!A{i}', [row])
            continue




class Coupon:
    def __init__(self, code, amount, price_rule_id, discount_code_id, usage_count, usage_limit):
        self.code = code
        self.amount = amount
        self.price_rule_id = price_rule_id
        self.discount_code_id = discount_code_id
        self.usage_count = usage_count
        self.usage_limit = usage_limit

def disable_coupon(coupon) -> bool:
    # Update the price rule to disable the coupon
    update_url = f"{store_url}/price_rules/{coupon.price_rule_id}.json"
    update_payload = {
        "price_rule": {
            "id": coupon.price_rule_id,
            "ends_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }
    }
    
    response = requests.put(update_url, json=update_payload, headers=shopify_headers)
    
    if response.status_code != 200:
        log("E: Error disabling coupon", indent=1)
        return False

    return True


def check_coupon(coupon_code, indent_level=0) -> Tuple[bool, Coupon, str, bool]:
    search_url = f"{store_url}/discount_codes/lookup.json"
    params = {'code': coupon_code}
    response = requests.get(search_url, headers=shopify_headers, params=params)
    
    if response.status_code != 200:
        coupon = Coupon(coupon_code, 0, None, None, None, None)
        return False, coupon, "Cannot find coupon", False
    
    discount_data = response.json().get('discount_code')
    
    if not discount_data:
        coupon = Coupon(coupon_code, 0, None, None, None, None)
        return False, coupon, "Tech Error", True

    discount_code_id = discount_data['id']
    # Get the price rule
    price_rule_id = discount_data['price_rule_id']
    price_rule_url = f"{store_url}/price_rules/{price_rule_id}.json"
    response = requests.get(price_rule_url, headers=shopify_headers)
    
    if response.status_code != 200:
        coupon = Coupon(coupon_code, 0, None, None, None, None)
        return False, coupon, "Tech Error", True
    
    price_rule = response.json()['price_rule']
    
    # Check if the coupon is expired
    current_time = datetime.datetime.now(datetime.timezone.utc)
    starts_at = datetime.datetime.fromisoformat(price_rule['starts_at'].replace('Z', '+00:00'))
    ends_at = datetime.datetime.fromisoformat(price_rule['ends_at'].replace('Z', '+00:00')) if price_rule['ends_at'] else None
    
    is_valid = starts_at <= current_time and (ends_at is None or current_time < ends_at)

    # Check if the usage is within limits
    usage_count = discount_data['usage_count']
    usage_limit = price_rule['usage_limit']

    if is_valid:
        coupon = Coupon(coupon_code, -1*float(price_rule['value']), price_rule_id, discount_code_id, int(usage_count), int(usage_limit))
        return True, coupon, None, False
    else:
        coupon = Coupon(coupon_code, 0, None, None, None, None)
        return False, coupon, "Coupon expired", False


def update_coupon(coupon, new_amount, usage_limit=None) -> bool:
    if usage_limit is None:
        usage_limit = coupon.usage_limit + 1

    # Update the price rule to update usage and amount
    update_url = f"{store_url}/price_rules/{coupon.price_rule_id}.json"
    update_payload = {
        "price_rule": {
            "id": coupon.price_rule_id,
            "usage_limit": str(usage_limit),
            "value": -1*new_amount
        }
    }
    
    response = requests.put(update_url, json=update_payload, headers=shopify_headers)
    
    if response.status_code != 200:
        log("E: Error updating coupon", indent=1)
        log(response.text, indent=2)
        return False

    coupon.amount = new_amount
    coupon.usage_limit = usage_limit
    return True


def log(message, indent=0):
    print(f"{' ' * (4 * indent)}{message}")


def color_and_update(header, row, index, sheet_id, sheet_name):
    red_rgb = (234,153,153)
    end_col = chr(ord('A') + len(header) - 1) 
    update_range_color(sheet_id, creds, f'{sheet_name}!A{index}:{end_col}{index}', red_rgb)
    write_data(sheet_id, creds, f'{sheet_name}!A{index}', [row])
   

    
if __name__ == "__main__":
    creds = authenticate()
    main(creds)


