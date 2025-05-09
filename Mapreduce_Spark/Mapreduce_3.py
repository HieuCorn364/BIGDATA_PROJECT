from Spark_config import get_spark_session
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, to_date, year, weekofyear

spark = get_spark_session(app_name="KhoiLuongGiaoDich")

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
except Exception as e:
    print(f"Error reading JSON file: {e}")
    spark.stop()
    exit(1)

weekly_volume_df = df \
    .filter(col("Ngày").isNotNull() & 
            col("Khối lượng khớp lệnh").isNotNull() & 
            col("Khối lượng thỏa thuận").isNotNull()) \
    .withColumn("Date", to_date(col("Ngày"), "dd/MM/yyyy")) \
    .withColumn("Năm", year(col("Date"))) \
    .withColumn("Tuần", weekofyear(col("Date"))) \
    .withColumn("Khối lượng khớp lệnh", col("Khối lượng khớp lệnh").cast("float")) \
    .withColumn("Khối lượng thỏa thuận", col("Khối lượng thỏa thuận").cast("float")) \
    .withColumn("Tổng Khối Lượng", col("Khối lượng khớp lệnh") + col("Khối lượng thỏa thuận")) \
    .groupBy("Năm", "Tuần") \
    .sum("Tổng Khối Lượng") \
    .withColumnRenamed("sum(Tổng Khối Lượng)", "Tổng Khối Lượng Giao Dịch") \
    .orderBy("Năm", "Tuần")

output_dir = "hdfs://localhost:9000/output/ree_3"
weekly_volume_df.coalesce(1).write.mode("overwrite").parquet(f"{output_dir}/weekly_volume")


print("Mapreduce bằng Spark DataFrame thành công rồi Hiếu ơi vào View_Parquet mà xem kết quả!!")
spark.stop()