import os
from datetime import datetime
from zoneinfo import ZoneInfo

from funcs import authenticate, get_data, append_data, parse_time

'''
This code reads from the RTO Management sheet and checks if any reship order is pending. If it is, it appends information to Manager Work sheet.
'''

def main():
    work_sheet_data = get_data(work_sheet_id, creds, 'Sheet1')
    header = work_sheet_data[0]
    for row in work_sheet_data[1:]:
        work_sheet_work_set.add(row[header.index('Work')])
    
    prepaid_data = get_data(rto_sheet_id, creds, 'Prepaid')
    process_data(prepaid_data, prepaid=True)

    cod_data = get_data(rto_sheet_id, creds, 'CoD-Reship')
    process_data(cod_data, prepaid=False)
    

def process_data(sheet_data, prepaid=True):
    global work_sheet_work_set

    now = parse_time(datetime.now())
    curr_datetime = now.strftime('%d-%m-%Y %H:%M:%S')
    
    local_system = os.getenv('LOCAL_SYSTEM')
    if local_system == False:
        curr_datetime_ist = now.astimezone(ZoneInfo(IST)).strftime('%d-%m-%Y %H:%M:%S')
    else:
        curr_datetime_ist = curr_datetime
    
    header = sheet_data[0]
    for row in sheet_data[1:]:
        row += [''] * (len(header) - len(row))
        order_number = row[header.index('Order No')]
        
        if not order_number:
            continue

        scan_date = row[header.index('Scan Date')]
        reship_tracking_link = row[header.index('Reship Tracking Link')]
        if prepaid:
            resolution = row[header.index('Resolution')]

        if scan_date and not reship_tracking_link:
            if (not prepaid) or (prepaid and resolution == 'Reship'):
                order_type = 'PREPAID' if prepaid else 'COD'
                msg = f'Reship {order_type} order {order_number} is pending.'
                if msg in work_sheet_work_set:
                    continue
                append_data(work_sheet_id, creds, "Sheet1", [[curr_datetime_ist, msg, ""]])
                work_sheet_work_set.add(msg)

if __name__ == '__main__':
    IST = ZoneInfo('Asia/Kolkata')
    creds = authenticate()
    rto_sheet_id = os.getenv('RTO_SPREADSHEET_ID')
    work_sheet_id = os.getenv('WORK_SPREADSHEET_ID')

    work_sheet_work_set = set()

    main()