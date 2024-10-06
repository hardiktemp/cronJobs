import os
import requests
from dotenv import load_dotenv
from pymongo import MongoClient
from dateutil.parser import isoparse

from shopify import store_url

load_dotenv()

client = MongoClient(os.getenv('MONGO_URI'))
shopify_url = os.getenv('SHOPIFY_API_KEY')

db = client['test']
collection = db['orders']


def get_order(order_id):
    url = store_url + f"orders/{order_id}.json"

    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['order']
    

def main():
    id = 6234972127515
    print(f"\nProcessing order {id}")
    print(f"    Reading from Shopify")
    order = get_order(id)
    if not order:
        print(f"    Order {id} not found.")
        return

    mongoorder = {"id": order['id'], 'order_number': int(order['order_number']) , 'created_at': isoparse(order['created_at']),}
    mongoorder['cancelled'] = isoparse(order['cancelled_at']) if order['cancelled_at'] else False
    mongoorder['price'] = float(order['current_total_price'])
    mongoorder['fullfilment_status'] = order['fulfillment_status']
    mongoorder['financial_status'] = order['financial_status']
    mongoorder["status_url"] = order['order_status_url'] 
    
    # Use update_one with upsert=True to insert the document if it doesn't exist, or update if it does
    result = collection.update_one(
        {'id': mongoorder['id']},  # Filter by order id
        {'$set': mongoorder},      # Update the document with new values
        upsert=True                # Insert the document if it doesn't exist
    )

    if result.matched_count > 0:
        print(f"    Order {mongoorder['order_number']} updated")
    else:
        print(f"    Order {mongoorder['order_number']} inserted")

if __name__ == '__main__':
    main()
