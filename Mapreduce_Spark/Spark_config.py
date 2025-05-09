from pyspark.sql import SparkSession

def get_spark_session(app_name="MapReduceApp"):
    # Cấu hình SparkSession
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.default.parallelism", "8") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .config("spark.hadoop.dfs.replication", "1") \
        .getOrCreate()
    
    # Thiết lập log level để giảm output không cần thiết
    spark.sparkContext.setLogLevel("WARN")
    return spark