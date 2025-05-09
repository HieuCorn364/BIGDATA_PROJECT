from Spark_config import get_spark_session
spark = get_spark_session("ViewResults")
df = spark.read.parquet("hdfs://localhost:9000/output/ree_4/Extremum_price")
df.show(100, truncate=False)
spark.stop()