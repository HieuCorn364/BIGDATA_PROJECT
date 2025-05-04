import requests
import pandas as pd
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

session = requests.Session()
retries = Retry(total=5, backoff_factor=2, status_forcelist=[503, 429])
session.mount('https://', HTTPAdapter(max_retries=retries))

def scrape_page_api(page_number, symbol="REE", page_size=200):
    # Đây là API của CafeF nè
    url = f"https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol={symbol}&StartDate=&EndDate=&PageIndex={page_number}&PageSize={page_size}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        if "Data" in data and "Data" in data["Data"] and len(data["Data"]["Data"]) > 0:
            rows = data["Data"]["Data"]
            total_count = data["Data"]["TotalCount"]
            total_pages = (total_count + page_size - 1) // page_size  # Tính tổng số trang

            page_data = []
            for row in rows:
                data_row = [
                    row["Ngay"],
                    str(row["GiaDongCua"]),
                    str(row["GiaDieuChinh"]),
                    row["ThayDoi"],
                    str(row["KhoiLuongKhopLenh"]),
                    str(row["GiaTriKhopLenh"] / 1000000000),
                    str(row["KLThoaThuan"]),
                    str(row["GtThoaThuan"] / 1000000000),
                    str(row["GiaMoCua"]),
                    str(row["GiaCaoNhat"]),
                    str(row["GiaThapNhat"])
                ]
                page_data.append(data_row)

            return page_data, total_pages
        else:
            print(f"Không tìm thấy dữ liệu ở trang {page_number}")
            return None, 0

    except requests.RequestException as e:
        print(f"Lỗi khi cào dữ liệu trang {page_number}: {e}")
        return None, 0

all_data = []
page = 1
total_pages = float('inf')
columns = ['Ngày', 'Giá đóng cửa', 'Giá điều chỉnh', 'Thay đổi', 'Khối lượng khớp lệnh',
           'Giá trị khớp lệnh', 'Khối lượng thỏa thuận', 'Giá trị thỏa thuận', 'Giá mở cửa', 'Giá cao nhất',
           'Giá thấp nhất']

try:
    df_existing = pd.read_csv('ree.csv')
    all_data = df_existing.values.tolist()
    page = (len(all_data) // 200) + 1
    print(f"Tiếp tục cào từ trang {page} (đã cào {len(all_data)} dòng trước đó)...")
except FileNotFoundError:
    print("Bắt đầu cào")

while page <= total_pages:
    print(f"Đang cào dữ liệu trang {page}...")
    page_data, total_pages = scrape_page_api(page)

    if not page_data:
        break

    all_data.extend(page_data)

    df = pd.DataFrame(all_data, columns=columns)
    df.to_csv('ree.csv', index=False)

    page += 1
    time.sleep(5)

df = pd.DataFrame(all_data, columns=columns)

df = df.drop_duplicates()

df.to_csv('ree.csv', index=False)

print("Dữ liệu đã được lưu vào ree.csv")
print(f"Tổng số dòng dữ liệu: {len(df)}")