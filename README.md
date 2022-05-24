# Data-engineering-store-mini-project

# Table of Content
1. [Project Overview](#project)
2. [Dataset Overview](#dataset)
3. [Exercise](#ex)
4. [Model Choose](#model)

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
How many products have been sold at least once? Which is the product contained in more orders?

<h3>Exercise 2</h3>
How many distinct products have been sold in each day?<br>

<h3>Exercise 3</h3>
What is the average revenue of the orders?<br>

<h3>Exercise 4</h3>
For each seller, what is the average % contribution of an order to the seller's daily quota?<br>

<h3>Exercise 5</h3>
Who are the <b>second most selling and the least selling</b> persons (sellers) for each product? Who are those for product with `product_id = 0`<br>

<h3>Exercise 6</h3>
Create a new column called "hashed_bill" defined as follows:<br>
- if the order_id is even: apply MD5 hashing iteratively to the bill_raw_text field, once for each 'A' (capital 'A') present in the text. E.g. if the bill text is 'nbAAnllA', you would apply hashing three times iteratively (only if the order number is even)<br>
- if the order_id is odd: apply SHA256 hashing to the bill text<br>
Finally, check if there are any duplicate on the new column<br>
