from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
import json
import os

# === BƯỚC 1: CHUYỂN FILE JSON ARRAY THÀNH FILE DÒNG ===
# Tên file gốc và file đã xử lý
original_file = "ree.json"
fixed_file = "ree_fixed.json"

# Chỉ xử lý nếu chưa có file fixed
if not os.path.exists(fixed_file):
    with open(original_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    with open(fixed_file, "w", encoding="utf-8") as f:
        for item in data:
            json.dump(item, f, ensure_ascii=False)
            f.write("\n")

# === BƯỚC 2: KHỞI TẠO SPARK SESSION ===
spark = SparkSession.builder.appName("PhanTichDuLieuChungKhoan_RDD").getOrCreate()
sc = spark.sparkContext

# === BƯỚC 3: ĐỊNH NGHĨA SCHEMA ===
schema = StructType([
    StructField("Ngày", StringType(), True),
    StructField("Giá đóng cửa", StringType(), True),
    StructField("Giá điều chỉnh", StringType(), True),
    StructField("Thay đổi", StringType(), True),
    StructField("Khối lượng khớp lệnh", StringType(), True),
    StructField("Giá trị khớp lệnh", StringType(), True),
    StructField("Khối lượng thỏa thuận", StringType(), True),
    StructField("Giá trị thỏa thuận", StringType(), True),
    StructField("Giá mở cửa", StringType(), True),
    StructField("Giá cao nhất", StringType(), True),
    StructField("Giá thấp nhất", StringType(), True)
])

# === BƯỚC 4: ĐỌC DỮ LIỆU TỪ FILE ĐÃ FIX ===
df = spark.read.schema(schema).json(fixed_file)
rdd = df.rdd

# === BƯỚC 5: XỬ LÝ VỚI RDD ===
def parse_record(record):
    try:
        if record["Ngày"] is None or record["Giá đóng cửa"] is None:
            return None
        date = datetime.strptime(record["Ngày"], "%d/%m/%Y")
        year = date.year
        month = date.month
        close_price = float(record["Giá đóng cửa"])
        return (year, month, close_price)
    except (ValueError, TypeError):
        return None

parsed_rdd = rdd.map(parse_record).filter(lambda x: x is not None)

# Trung bình theo tháng
monthly_avg_rdd = parsed_rdd.map(lambda x: ((x[0], x[1]), (x[2], 1))) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .map(lambda x: (x[0][0], x[0][1], x[1][0] / x[1][1])) \
    .sortBy(lambda x: (x[0], x[1]))

# Cao nhất và thấp nhất theo năm
yearly_extremes_rdd = parsed_rdd.map(lambda x: (x[0], x[2])) \
    .groupByKey() \
    .map(lambda x: (x[0], (max(x[1]), min(x[1])))) \
    .sortBy(lambda x: x[0])

# Chuyển về DataFrame
monthly_avg_df = spark.createDataFrame(
    monthly_avg_rdd,
    ["Năm", "Tháng", "Giá_Đóng_Cửa_Trung_Bình"]
)

yearly_extremes_df = spark.createDataFrame(
    yearly_extremes_rdd.map(lambda x: (x[0], x[1][0], x[1][1])),
    ["Năm", "Giá_Đóng_Cửa_Cao_Nhất", "Giá_Đóng_Cửa_Thấp_Nhất"]
)

# Hiển thị kết quả
print("Giá Đóng Cửa Trung Bình Theo Tháng (RDD):")
monthly_avg_df.show()

print("Giá Đóng Cửa Cao Nhất và Thấp Nhất Theo Năm (RDD):")
yearly_extremes_df.show()

spark.stop()
