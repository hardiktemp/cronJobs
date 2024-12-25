import os
from dotenv import load_dotenv
from pymongo import MongoClient

from funcs import authenticate, get_data, clean_phone

load_dotenv()


def main():
    client = MongoClient(os.getenv('MONGO_URI'))
    db = client['test']
    collection = db['removeData']

    sheet_id = os.getenv('CUSTOMER_SUPPORT_AUTOMATION_ID')
    sheet_name = 'Remove Customer Data'
    creds = authenticate()
    sheet_data = get_data(sheet_id, creds, sheet_name)
    header = sheet_data[0]

    phones_to_add = set()
    phone_index = header.index('Phone Number*')
    
    for row in sheet_data[1:]:
        phone = clean_phone(row[phone_index])
        if phone:
            phones_to_add.add(phone)
    
    # delete all old records
    collection.delete_many({})

    # insert new records
    if phones_to_add:
        documents_to_insert = [{"phone": phone} for phone in phones_to_add]
        collection.insert_many(documents_to_insert)
    
    print(f"Successfully updated {len(phones_to_add)} records.")

    print("Adding users in Blacklist")
    client = MongoClient(os.getenv('MONGO_URI_BLACKLIST'))
    db = client['test'] 
    collection = db['blacklistedmembers']

    for row in sheet_data[1:]:
        phone = clean_phone(row[phone_index]) 
        if collection.find_one({"Phone": phone}):
            continue

        collection.insert_one({"Phone": phone})
    
    print("Successfully added users in Blacklist")


if __name__ == '__main__':
    main()
