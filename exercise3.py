from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.types import IntegerType

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


# Step 1 - Check and select the skewed keys
# In this case we are retrieving the top 100 keys: these will be the only salted keys.
results = sales_table.groupby(sales_table["product_id"]).count().sort(col("count").desc()).limit(100).collect()

# Step 2 - What we want to do is:
#  a. Duplicate the entries that we have in the dimension table for the most common products, e.g.
#       product_0 will become: product_0-1, product_0-2, product_0-3 and so on
#  b. On the sales table, we are going to replace "product_0" with a random duplicate (e.g. some of them
#     will be replaced with product_0-1, others with product_0-2, etc.)
# Using the new "salted" key will unskew the join

# Let's create a dataset to do the trick
REPLICATION_FACTOR = 101
l = []
replicated_products = []
for _r in results:
    replicated_products.append(_r["product_id"])
    for _rep in range(0, REPLICATION_FACTOR):
        l.append((_r["product_id"], _rep))
rdd = spark.sparkContext.parallelize(l)
replicated_df = rdd.map(lambda x: Row(product_id=x[0], replication=int(x[1])))
replicated_df = spark.createDataFrame(replicated_df)

#   Step 3: Generate the salted key
products_table = products_table.join(broadcast(replicated_df),
                                     products_table["product_id"] == replicated_df["product_id"], "left"). \
    withColumn("salted_join_key", when(replicated_df["replication"].isNull(), products_table["product_id"]).otherwise(
    concat(replicated_df["product_id"], lit("-"), replicated_df["replication"])))

sales_table = sales_table.withColumn("salted_join_key", when(sales_table["product_id"].isin(replicated_products),
                                                             concat(sales_table["product_id"], lit("-"),
                                                                    round(rand() * (REPLICATION_FACTOR - 1), 0).cast(
                                                                        IntegerType()))).otherwise(
    sales_table["product_id"]))

#   Step 4: Finally let's do the join
print(sales_table.join(products_table, sales_table["salted_join_key"] == products_table["salted_join_key"],
                       "inner").
      agg(avg(products_table["price"] * sales_table["num_pieces_sold"])).show())

print("Ok")