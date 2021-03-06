# Data-engineering-store-mini-project

# Table of Content
1. [Project Overview](#project)
2. [Dataset Overview](#dataset)
3. [Exercise](#ex)
4. [Conclusion](#con)

<a name="project"></a>
## Project Overview
Extract Insightfull information from Products, Sales and Sellers table. Spark is implement for table joins and handle skew datasets.<br>

<a name="dataset"></a>
## Dataset Overview
it consists of three tables coming from the database of a shop, with products, sales and sellers. Data is available in Parquet files<br>

Relations of table can be understand using following diagram:<br>
![image](https://user-images.githubusercontent.com/17496623/169950981-25589fd2-93bb-42af-aa40-22c66c45762c.png)
<h3>Sales Table</h3>
Each row in this table is an order and every order can contain only one product. Each row stores the following fields:
<ul>
<li>order_id: The order ID</li>
<li>product_id: The single product sold in the order. All orders have exactly one product)</li>
<li>seller_id: The selling employee ID that sold the product</li>
<li>num_pieces_sold: The number of units sold for the specific product in the order</li>
<li>bill_raw_text: A string that represents the raw text of the bill associated with the order</li>
<li>date: The date of the order.</li>
</ul>

<h3>Products Table</h3>
Each row represents a distinct product. The fields are:
<ul>
<li>product_id: The product ID</li>
<li>product_name: The product name</li>
<li>price: The product price</li>
</ul>
<h3>Sellers Table</h3>
This table contains the list of all the sellers:
<ul>
  <li>seller_id: The seller ID</li>
<li>seller_name: The seller name</li>
<li>daily_target: The number of items (regardless of the product type) that the seller needs to hit his/her quota. For example, if the daily target is 100,000, the employee needs to sell 100,000 products he can hit the quota by selling 100,000 units of product_0, but also selling 30,000 units of product_1 and 70,000 units of product_2</li>
</ul>

<a name="ex"></a>
## Exercise

<h3>Exercise 1</h3>
Find out how many orders, how many products and how many sellers are in the data.<br>
How many products have been sold at least once? Which is the product contained in more orders?<br>
<h4>Solutions:</h4>
First, we simply need to count how many rows we have in every dataset:<br>

<img src='https://user-images.githubusercontent.com/17496623/170081640-243c51ac-a909-469e-a4ce-ebbfdac6ead0.png'>

As we can see, There are <b>75,000,000</b> products in our dataset and <b>20,000,040</b> orders <br>
since each order can only have a single product, some of them have never been sold.<br>
```
#   Output how many products have been actually sold at least once
print("Number of products sold at least once")
sales_table.agg(countDistinct(col("product_id"))).show()

#   Output which is the product that has been sold in more orders
print("Product present in more orders")
sales_table.groupBy(col("product_id")).agg(
    count("*").alias("cnt")).orderBy(col("cnt").desc()).limit(1).show()
```
<p>The first query is counting how many distinct products we have in the sales table, while the second block is pulling the product_id that has the highest count in the sales table.</p>
<! --![image](https://user-images.githubusercontent.com/17496623/170075200-ec25108f-804b-44f1-8dd2-22228288bd47.png)-->
<img src='https://user-images.githubusercontent.com/17496623/170075200-ec25108f-804b-44f1-8dd2-22228288bd47.png'>
<p>Let???s have a closer look at the second result: 19,000,000 orders out of 20 M are selling the product with product_id = 0: this is a powerful information that we should use later!</p>
<a href='https://github.com/pratikp12/Data-engineering-store-mini-project/blob/main/exercise1.py'>Code</a>
<h3>Exercise 2</h3>
How many distinct products have been sold in each day?<br>
<h4>Solutions:</h4>

```
sales_table.groupby(col("date")).agg(countDistinct(col("product_id")).alias("distinct_products_sold")).orderBy(col("distinct_products_sold").desc()).show()
```
<br>
<img src='https://user-images.githubusercontent.com/17496623/170082241-698572a3-b54a-4816-b78c-b78e55af2660.png'>
<a href='https://github.com/pratikp12/Data-engineering-store-mini-project/blob/main/exercise2.py'>Code</a>
<h3>Exercise 3</h3>
What is the average revenue of the orders?<br>
<h4>Solutions:</h4>
<p>we first need to calculate the revenue for each order and then get the average. Remember that revenue = price * quantity. Petty easy: the product_price is in the products table, while the amount is in the sales table.</p>

```
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
```

<img src='https://user-images.githubusercontent.com/17496623/170077224-b5faf288-d1a6-4ada-9d1b-6351d1d32d71.png'>
<a href='https://github.com/pratikp12/Data-engineering-store-mini-project/blob/main/exercise3.py'>Code</a>
<h3>Exercise 4</h3>
For each seller, what is the average % contribution of an order to the seller's daily quota?<br>
<h4>Solutions:</h4>
<p>we can join our table with the sellers table, we calculate the percentage of the quota hit thanks to a specific order and we do the average, grouping by the seller_id.
</p>
<p>Again, this could generate a skewed join, since even the sellers are not evenly distributed. In this case, though, the solution is much simpler! Since the sellers table is very small, we can broadcast it, making the operations much much faster!</p>

<p>???Broadcasting??? simply means that a copy of the table is sent to every executor, allowing to ???localize??? the task. We need to use this operator carefully: when we broadcast a table, we need to be sure that this will not become too-big-to-broadcast in the future, otherwise we???ll start to have Out Of Memory errors later in time (as the broadcast dataset gets bigger).</p>

```
#   Correct way through broarcasting
print(sales_table.join(broadcast(sellers_table), sales_table["seller_id"] == sellers_table["seller_id"], "inner").withColumn(
    "ratio", sales_table["num_pieces_sold"]/sellers_table["daily_target"]
).groupBy(sales_table["seller_id"]).agg(avg("ratio")).show())
```

<img src='https://user-images.githubusercontent.com/17496623/170078562-1d75f2e4-b82a-4d50-970a-9030f2aa14bb.png'>
<a href='https://github.com/pratikp12/Data-engineering-store-mini-project/blob/main/exercise4.py'>Code</a>
<h3>Exercise 5</h3>
Who are the <b>second most selling and the least selling</b> persons (sellers) for each product? Who are those for product with `product_id = 0`<br>
<h4>Solutions:</h4>
<p>for each product, we need the second most selling and the least selling employees (sellers): we are probably going to need two rankings, one to get the second and the other one to get the last in the sales chart. We also need to handle some edge cases:</p>
<ul>
  <li>
If a product has been sold by only one seller, we???ll put it into a special category (category: Only seller or multiple sellers with the same quantity).</li>
<li>If a product has been sold by more than one seller, but all of them sold the same quantity, we are going to put them in the same category as if they were only a single seller for that product (category: Only seller or multiple sellers with the same quantity).</li>
<li>If the ???least selling??? is also the ???second selling???, we will count it only as ???second seller???</li>
  </ul>
  Let???s draft a strategy:<br>
  <ol>
  <li>  We get the sum of sales for each product and seller pairs.  </li>
  <li>We add two new ranking columns: one that ranks the products??? sales in descending order and another one that ranks in ascending order.  </li>
  <li>We split the dataset obtained in three pieces: one for each case that we want to handle (second top selling, least selling, single selling).  </li>
  <li>When calculating the ???least selling???, we exclude those products that have a single seller and those where the least selling employee is also the second most selling  </li>
  <li>We merge the pieces back together.  </li>
    </ol>


```
#Calcuate the number of pieces sold by each seller for each product
sales_table = sales_table.groupby(col("product_id"), col("seller_id")). \
    agg(sum("num_pieces_sold").alias("num_pieces_sold"))
# Create the window functions, one will sort ascending the other one descending. Partition by the product_id
# and sort by the pieces sold
window_desc = Window.partitionBy(col("product_id")).orderBy(col("num_pieces_sold").desc())
window_asc = Window.partitionBy(col("product_id")).orderBy(col("num_pieces_sold").asc())
# Create a Dense Rank (to avoid holes)
sales_table = sales_table.withColumn("rank_asc", dense_rank().over(window_asc)). \
    withColumn("rank_desc", dense_rank().over(window_desc))

# Get products that only have one row OR the products in which multiple sellers sold the same amount
# (i.e. all the employees that ever sold the product, sold the same exact amount)
single_seller = sales_table.where(col("rank_asc") == col("rank_desc")).select(
    col("product_id").alias("single_seller_product_id"), col("seller_id").alias("single_seller_seller_id"),
    lit("Only seller or multiple sellers with the same results").alias("type")
)

# Get the second top sellers
second_seller = sales_table.where(col("rank_desc") == 2).select(
    col("product_id").alias("second_seller_product_id"), col("seller_id").alias("second_seller_seller_id"),
    lit("Second top seller").alias("type")
)

# Get the least sellers and exclude those rows that are already included in the first piece
# We also exclude the "second top sellers" that are also "least sellers"
least_seller = sales_table.where(col("rank_asc") == 1).select(
    col("product_id"), col("seller_id"),
    lit("Least Seller").alias("type")
).join(single_seller, (sales_table["seller_id"] == single_seller["single_seller_seller_id"]) & (
        sales_table["product_id"] == single_seller["single_seller_product_id"]), "left_anti"). \
    join(second_seller, (sales_table["seller_id"] == second_seller["second_seller_seller_id"]) & (
        sales_table["product_id"] == second_seller["second_seller_product_id"]), "left_anti")

# Union all the pieces
union_table = least_seller.select(
    col("product_id"),
    col("seller_id"),
    col("type")
).union(second_seller.select(
    col("second_seller_product_id").alias("product_id"),
    col("second_seller_seller_id").alias("seller_id"),
    col("type")
)).union(single_seller.select(
    col("single_seller_product_id").alias("product_id"),
    col("single_seller_seller_id").alias("seller_id"),
    col("type")
))
union_table.show()

# Which are the second top seller and least seller of product 0?
union_table.where(col("product_id") == 0).show()
```

sample output
<!--[image](https://user-images.githubusercontent.com/17496623/170072334-98807e84-e67f-4291-9baa-e80561a1ef95.png)-->

<br>next<br>
![image](https://user-images.githubusercontent.com/17496623/170072500-f4b3db72-08f1-4d60-8152-b479ab00b8bd.png)
<a href='https://github.com/pratikp12/Data-engineering-store-mini-project/blob/main/exercise5.py'>Code</a>

<h3>Exercise 6</h3>
Create a new column called "hashed_bill" defined as follows:<br>
- if the order_id is even: apply MD5 hashing iteratively to the bill_raw_text field, once for each 'A' (capital 'A') present in the text. E.g. if the bill text is 'nbAAnllA', you would apply hashing three times iteratively (only if the order number is even)<br>
- if the order_id is odd: apply SHA256 hashing to the bill text<br>
Finally, check if there are any duplicate on the new column<br>
<h4>Solutions:</h4>


<p>First, we need to define the UDF function: def algo(order_id, bill_text). The algo function receives the order_id and the bill_text as input.</p>
The UDF function implements the algorithm:<br>
<ol>
 <li>Check if the order_id is even or odd.</li>
<li>If order_id is even, count the number of capital ???A??? in the bill text and iteratively apply MD5</li>
  <li>If order_id is odd, apply SHA256</li>
  <li>Return the hashed string</li>
  </ol>
  <p>Afterward, this function needs to be registered in the Spark Session through the line algo_udf = spark.udf.register(???algo???, algo). The first parameter is the name of the function within the Spark context while the second parameter is the actual function that will be executed.</p>
  We apply the UDF at the following line:<br>
  
```
sales_table.withColumn("hashed_bill", algo_udf(col("order_id"), col("bill_raw_text")))
```
<img src='https://user-images.githubusercontent.com/17496623/170084469-631d4133-6fcd-4808-9655-551de881aeb4.png'>
<a href='https://github.com/pratikp12/Data-engineering-store-mini-project/blob/main/exercise6.py'>Code</a>
<a name="con"></a>
## Conclusion
we learn following important Topics of Spark SQL:
<ol>
<li>Joins Skewness: This is usually the main pain point in Spark pipelines; sometimes it is very difficult to solve, because it???s not easy to find a balance among all the factors that are involved in these operations.</li>
<li>Window functions: Super useful, the only thing to remember is to first define the windowing.</li>
<li>UDFs: Although they are very helpful, we should think twice before jumping into the development of such functions, since their execution might slow down our code.</li>
</ol>
