import requests
import json
import time
import os

# Thiết lập session
session = requests.Session()

def scrape_page_api(page_number, symbol="REE", page_size=200, max_retries=3):
    # API của CafeF
    url = f"https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol={symbol}&StartDate=&EndDate=&PageIndex={page_number}&PageSize={page_size}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    for attempt in range(max_retries):
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
                    data_row = {
                        "Ngày": row["Ngay"],
                        "Giá đóng cửa": str(row["GiaDongCua"]),
                        "Giá điều chỉnh": str(row["GiaDieuChinh"]),
                        "Thay đổi": row["ThayDoi"],
                        "Khối lượng khớp lệnh": str(row["KhoiLuongKhopLenh"]),
                        "Giá trị khớp lệnh": str(row["GiaTriKhopLenh"] / 1000000000),
                        "Khối lượng thỏa thuận": str(row["KLThoaThuan"]),
                        "Giá trị thỏa thuận": str(row["GtThoaThuan"] / 1000000000),
                        "Giá mở cửa": str(row["GiaMoCua"]),
                        "Giá cao nhất": str(row["GiaCaoNhat"]),
                        "Giá thấp nhất": str(row["GiaThapNhat"])
                    }
                    page_data.append(data_row)

                return page_data, total_pages
            else:
                print(f"Không tìm thấy dữ liệu ở trang {page_number}")
                return None, 0

        except requests.RequestException as e:
            print(f"Lỗi khi cào dữ liệu trang {page_number} (lần thử {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(10 * (attempt + 1))  # Tăng thời gian chờ cho mỗi lần thử
            continue

    print(f"Đã thử {max_retries} lần cho trang {page_number} nhưng không thành công.")
    return None, 0

# Đường dẫn lưu file JSON
output_dir = "./data"
output_file = os.path.join(output_dir, "ree.json")

# Tạo thư mục nếu chưa tồn tại
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Khởi tạo danh sách dữ liệu
all_data = []
page = 1
total_pages = float('inf')

# Kiểm tra dữ liệu JSON hiện có
try:
    with open(output_file, 'r', encoding='utf-8') as f:
        all_data = json.load(f)
    page = (len(all_data) // 200) + 1
    print(f"Tiếp tục cào từ trang {page} (đã cào {len(all_data)} dòng trước đó)...")
except FileNotFoundError:
    print("Bắt đầu cào dữ liệu")

# Vòng lặp crawl dữ liệu
while page <= total_pages:
    print(f"Đang cào dữ liệu trang {page}...")
    page_data, total_pages = scrape_page_api(page)

    if not page_data:
        break

    all_data.extend(page_data)

    # Lưu dữ liệu vào file JSON
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=4)

    page += 1
    time.sleep(10)  # Tăng thời gian chờ lên 10 giây

# Loại bỏ dữ liệu trùng lặp
unique_data = []
seen = set()
for item in all_data:
    item_tuple = tuple(item.items())  # Chuyển dict thành tuple để so sánh
    if item_tuple not in seen:
        seen.add(item_tuple)
        unique_data.append(item)

# Lưu dữ liệu cuối cùng vào file JSON
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(unique_data, f, ensure_ascii=False, indent=4)

print(f"Dữ liệu đã được lưu vào {output_file}")
print(f"Tổng số dòng dữ liệu: {len(unique_data)}")
print(f"Số dòng trùng lặp đã loại bỏ: {len(all_data) - len(unique_data)}")