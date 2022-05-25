from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("Exercise1") \
    .getOrCreate()

#   Read the source tables in Parquet format
products_table = spark.read.parquet("F:/DatasetToCompleteTheSixSparkExercises/products_parquet")
sales_table = spark.read.parquet("F:/DatasetToCompleteTheSixSparkExercises/sales_parquet")
sellers_table = spark.read.parquet("F:/DatasetToCompleteTheSixSparkExercises/sellers_parquet")

sales_table.groupby(col("date")).agg(countDistinct(col("product_id")).alias("distinct_products_sold")).orderBy(
    col("distinct_products_sold").desc()).show()
