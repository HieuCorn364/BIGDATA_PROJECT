import json
import csv
import re
from datetime import datetime
import pandas as pd

def is_valid_date(date_str):
    try:
        datetime.strptime(date_str, '%d/%m/%Y')
        return True
    except ValueError:
        return False

def parse_change(change_str):
    match = re.match(r'([-\d.]+)\(([-.\d]+) %\)', change_str)
    if match:
        return float(match.group(1)), float(match.group(2))
    return 0.0, 0.0

with open('D:/IT_HCMUTE/Hoc_ki_6/BIG_DATA/BIGDATA_PROJECT/CrawlDataLib/data/ree.json', 'r', encoding='utf-8') as f:    data = json.load(f)

cleaned_data = []
seen_dates = set()

for i, record in enumerate(data):
    date = record['Ngày']
    if date in seen_dates:
        print(f"Bỏ qua bản ghi trùng lặp: {date}")
        continue
    seen_dates.add(date)

    if not is_valid_date(date):
        print(f"Bỏ qua bản ghi có ngày không hợp lệ: {date}")
        continue

    try:
        close_price = float(record['Giá đóng cửa'])
        adjusted_price = float(record['Giá điều chỉnh'])
        open_price = float(record['Giá mở cửa'])
        high_price = float(record['Giá cao nhất'])
        low_price = float(record['Giá thấp nhất'])
        matched_volume = int(record['Khối lượng khớp lệnh'])
        matched_value = float(record['Giá trị khớp lệnh'])
        agreed_volume = int(record['Khối lượng thỏa thuận'])
        agreed_value = float(record['Giá trị thỏa thuận'])
    except ValueError:
        print(f"Bỏ qua bản ghi có giá trị số không hợp lệ: {date}")
        continue

    if (close_price < 0 or adjusted_price < 0 or open_price < 0 or
        high_price < 0 or low_price < 0 or matched_volume < 0 or
        matched_value < 0 or agreed_volume < 0 or agreed_value < 0):
        print(f"Bỏ qua bản ghi có giá trị âm: {date}")
        continue

    if high_price < low_price:
        print(f"Bỏ qua bản ghi có giá cao nhất < giá thấp nhất: {date}")
        continue
    if not (low_price <= close_price <= high_price):
        print(f"Bỏ qua bản ghi có giá đóng cửa không hợp lý: {date}")
        continue

    change_value, change_percent = parse_change(record['Thay đổi'])

    id = f"{date.replace('/', '-')}_{i}"

    cleaned_data.append({
        'id': id,
        'date': date,
        'closePrice': close_price,
        'adjustedPrice': adjusted_price,
        'change': record['Thay đổi'],
        'changeValue': change_value,
        'changePercent': change_percent,
        'matchedVolume': matched_volume,
        'matchedValue': matched_value,
        'agreedVolume': agreed_volume,
        'agreedValue': agreed_value,
        'openPrice': open_price,
        'highPrice': high_price,
        'lowPrice': low_price
    })

df = pd.DataFrame(cleaned_data)

df['date_obj'] = pd.to_datetime(df['date'], format='%d/%m/%Y')
df = df.sort_values('date_obj').drop(columns=['date_obj'])

csv_file = 'D:/IT_HCMUTE/Hoc_ki_6/BIG_DATA/BIGDATA_PROJECT/CleanData_Load_Storage/ree_cleaned.csv'
df.to_csv(csv_file, index=False, encoding='utf-8')

print(f"Đã tạo file CSV hậu xử lý: {csv_file}")
print(f"Số bản ghi sau làm sạch: {len(df)}")