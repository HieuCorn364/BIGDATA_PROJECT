from Spark_config import get_spark_session
from pyspark.sql.types import StructType, StructField, StringType

spark = get_spark_session(app_name="BienDongGia")
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

# Xử lý bản ghi của file 
def parse_movement(record):
    try:
        change = record["Thay đổi"]
        if change is None:
            return None
        match = re.match(r"([-]?\d*\.?\d+)", change)
        if not match:
            return None
        value = float(match.group(1))
        if value > 0:
            return ("Tăng", 1)
        elif value < 0:
            return ("Giảm", 1)
        else:
            return ("Không đổi", 1)
    except (ValueError, TypeError) as e:
        print(f"Error parsing record: {record}, error: {e}")
        return None

movement_rdd = rdd.map(parse_movement) \
    .filter(lambda x: x is not None) \
    .reduceByKey(lambda x, y: x + y) \
    .cache()

tong_ngay_rdd = movement_rdd.map(lambda x: x[1]).reduce(lambda x, y: x + y)

phan_tram_rdd = movement_rdd.map(lambda x: (x[0], round((x[1] / tong_ngay_rdd) * 100, 2)))

percent_df = spark.createDataFrame(
    phan_tram_rdd,
    ["Loại biến động", "Phần trăm (%)"]
)

output_dir = "hdfs://localhost:9000/output/ree_2"
percent_df.write.mode("overwrite").parquet(f"{output_dir}/percentage")

print("Mapreduce thành công rồi Hiếu!!!!, qua bên View_Parquet mà coi kết quả nhé")

spark.stop()