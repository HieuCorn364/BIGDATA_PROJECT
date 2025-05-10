import jaydebeapi
import csv
import sys

try:
    conn = jaydebeapi.connect(
        'org.apache.phoenix.jdbc.PhoenixDriver',
        'jdbc:phoenix:localhost',
        [],
        'D:/IT_HCMUTE/Hoc_ki_6/BIG_DATA/BIGDATA_PROJECT/CleanData_Load_Storage/phoenix-4.14.3-HBase-1.4-client.jar'
    )
    cursor = conn.cursor()
    print("Kết nối Phoenix thành công!")
except Exception as e:
    print(f"Lỗi kết nối Phoenix: {e}")
    sys.exit(1)

csv_file = 'D:/IT_HCMUTE/Hoc_ki_6/BIG_DATA/BIGDATA_PROJECT/CleanData_Load_Storage/ree_cleaned.csv'
try:
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        upsert_query = """
        UPSERT INTO ree (
            id, date, close_price, adjusted_price, change, change_value, change_percent,
            matched_volume, matched_value, agreed_volume, agreed_value, open_price, high_price, low_price
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        for row in reader:
            try:
                cursor.execute(upsert_query, [
                    row['id'],
                    row['date'],
                    float(row['closePrice']),
                    float(row['adjustedPrice']),
                    row['change'],
                    float(row['changeValue']),
                    float(row['changePercent']),
                    int(row['matchedVolume']),
                    float(row['matchedValue']),
                    int(row['agreedVolume']),
                    float(row['agreedValue']),
                    float(row['openPrice']),
                    float(row['highPrice']),
                    float(row['lowPrice'])
                ])
            except Exception as e:
                print(f"Lỗi khi nhập bản ghi {row['id']}: {e}")
                continue

        conn.commit()
        print(f"Đã nhập dữ liệu từ {csv_file} vào Phoenix!")
except Exception as e:
    print(f"Lỗi khi nhập dữ liệu: {e}")
finally:
    cursor.close()
    conn.close()