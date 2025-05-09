from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime

spark = SparkSession.builder \
    .appName("PhanTichDuLieuChungKhoan_RDD") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()
sc = spark.sparkContext

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

try:
    df = spark.read.schema(schema).json("hdfs://localhost:9000/input/ree_fixed.json")
    rdd = df.rdd
except Exception as e:
    print(f"Error reading JSON file: {e}")
    spark.stop()
    exit(1)

def parse_record(record):
    try:
        if record["Ngày"] is None or record["Giá đóng cửa"] is None:
            return None
        date = datetime.strptime(record["Ngày"], "%d/%m/%Y")
        year = date.year
        month = date.month
        close_price = float(record["Giá đóng cửa"])
        return (year, month, close_price)
    except (ValueError, TypeError) as e:
        print(f"Error parsing record: {record}, error: {e}")
        return None

parsed_rdd = rdd.map(parse_record).filter(lambda x: x is not None).cache()

# Trung bình theo tháng
monthly_avg_rdd = parsed_rdd.map(lambda x: ((x[0], x[1]), (x[2], 1))) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .map(lambda x: (x[0][0], x[0][1], round(x[1][0] / x[1][1], 2))) \
    .sortBy(lambda x: (x[0], x[1]))

# Cao nhất và thấp nhất theo năm
min_max_price_yearly = parsed_rdd.map(lambda x: (x[0], x[2])) \
    .groupByKey() \
    .map(lambda x: (x[0], (round(max(x[1]), 2), round(min(x[1]), 2)))) \
    .sortBy(lambda x: x[0])

monthly_avg_df = spark.createDataFrame(
    monthly_avg_rdd,
    ["Năm", "Tháng", "Giá_Đóng_Cửa_Trung_Bình"]
)

yearly_extremes_df = spark.createDataFrame(
    min_max_price_yearly.map(lambda x: (x[0], x[1][0], x[1][1])),
    ["Năm", "Giá_Đóng_Cửa_Cao_Nhất", "Giá_Đóng_Cửa_Thấp_Nhất"]
)

output_dir = "hdfs://localhost:9000/output/ree_1"
monthly_avg_df.write.mode("overwrite").parquet(f"{output_dir}/monthly_avg")
yearly_extremes_df.write.mode("overwrite").parquet(f"{output_dir}/min_max_yearly")

print("Mapreduce đã được chạy thành công rồi Hiếu, giờ qua View_Parquet để xem kết quả")
spark.stop()