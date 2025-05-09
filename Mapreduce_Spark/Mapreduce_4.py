from Spark_config import get_spark_session
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, to_date, year, max, min

spark = get_spark_session(app_name="YearlyExtremesDatesDataFrame")

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

df_min_max_price_date = df \
    .filter(col("Ngày").isNotNull() & col("Giá đóng cửa").isNotNull()) \
    .withColumn("Date", to_date(col("Ngày"), "dd/MM/yyyy")) \
    .withColumn("Năm", year(col("Date"))) \
    .withColumn("Giá đóng cửa", col("Giá đóng cửa").cast("float"))

yearly_extremes = df_min_max_price_date \
    .groupBy("Năm") \
    .agg(
        max("Giá đóng cửa").alias("Giá Cao Nhất"),
        min("Giá đóng cửa").alias("Giá Thấp Nhất")
    )

df_alias = df_min_max_price_date.alias("df")
yearly_extremes_alias = yearly_extremes.alias("extremes")

max_dates = df_alias.join(
    yearly_extremes_alias,
    (col("df.Năm") == col("extremes.Năm")) & 
    (col("df.Giá đóng cửa") == col("extremes.Giá Cao Nhất")),
    "inner"
).select(
    col("df.Năm"),
    col("df.Ngày").alias("Ngày Cao Nhất"),
    col("df.Giá đóng cửa").alias("Giá Cao Nhất")
).distinct()

min_dates = df_alias.join(
    yearly_extremes_alias,
    (col("df.Năm") == col("extremes.Năm")) & 
    (col("df.Giá đóng cửa") == col("extremes.Giá Thấp Nhất")),
    "inner"
).select(
    col("df.Năm"),
    col("df.Ngày").alias("Ngày Thấp Nhất"),
    col("df.Giá đóng cửa").alias("Giá Thấp Nhất")
).distinct()

max_dates_alias = max_dates.alias("max_dates")
min_dates_alias = min_dates.alias("min_dates")

result_df = max_dates_alias.join(
    min_dates_alias,
    col("max_dates.Năm") == col("min_dates.Năm"),
    "outer"
).select(
    col("max_dates.Năm"),
    col("max_dates.Ngày Cao Nhất"),
    col("max_dates.Giá Cao Nhất"),
    col("min_dates.Ngày Thấp Nhất"),
    col("min_dates.Giá Thấp Nhất")
).orderBy("Năm")


output_dir = "hdfs://localhost:9000/output/ree_4"
result_df.coalesce(1).write.mode("overwrite").parquet(f"{output_dir}/Extremum_price")

print("Mệt quá đi, xong cái cuối rồi nè!!!!")
spark.stop()