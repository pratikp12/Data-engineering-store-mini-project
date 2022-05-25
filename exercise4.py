#   For each seller find the average % of the target amount brought by each order

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.types import IntegerType

# Create the Spark session
spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "3g") \
    .appName("Exercise1") \
    .getOrCreate()

# Read the source tables
products_table = spark.read.parquet("F:/DatasetToCompleteTheSixSparkExercises/products_parquet")
sales_table = spark.read.parquet("F:/DatasetToCompleteTheSixSparkExercises/sales_parquet")
sellers_table = spark.read.parquet("F:/DatasetToCompleteTheSixSparkExercises/sellers_parquet")

#   Wrong way to do this - Skewed
#   (Note that Spark will probably broadcast the table anyway, unless we forbid it throug the configuration paramters)
print(sales_table.join(sellers_table, sales_table["seller_id"] == sellers_table["seller_id"], "inner").withColumn(
    "ratio", sales_table["num_pieces_sold"]/sellers_table["daily_target"]
).groupBy(sales_table["seller_id"]).agg(avg("ratio")).show())

#   Correct way through broarcasting
print(sales_table.join(broadcast(sellers_table), sales_table["seller_id"] == sellers_table["seller_id"], "inner").withColumn(
    "ratio", sales_table["num_pieces_sold"]/sellers_table["daily_target"]
).groupBy(sales_table["seller_id"]).agg(avg("ratio")).show())