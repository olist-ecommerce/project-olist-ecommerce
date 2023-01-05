# Databricks notebook source
# Python code to mount and access Azure Data Lake Storage Gen2 Account to Azure Databricks with Service Principal and OAuth
# Author: Dhyanendra Singh Rathore

# Define the variables used for creating connection strings
adlsAccountName = "targetgroup1"
adlsContainerName = "output"
adlsFolderName = "raw"
mountPoint = "/mnt/csvFiles"

# Application (Client) ID
applicationId = dbutils.secrets.get(scope="NewScope",key="ClientId")

# Application (Client) Secret Key
authenticationKey = dbutils.secrets.get(scope="NewScope",key="ClientSecrets")

# Directory (Tenant) ID
tenandId = dbutils.secrets.get(scope="NewScope",key="TenantId")

endpoint = "https://login.microsoftonline.com/" + tenandId + "/oauth2/token"
source = "abfss://" + adlsContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/" + adlsFolderName

# Connecting using Service Principal secrets and OAuth
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": applicationId,
           "fs.azure.account.oauth2.client.secret": authenticationKey,
           "fs.azure.account.oauth2.client.endpoint": endpoint}

# Mounting ADLS Storage to DBFS
# Mount only if the directory is not already mounted
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()): 
   dbutils.fs.mount(
    source = source,
    mount_point = mountPoint,
    extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs ls '/mnt/csvFiles/'

# COMMAND ----------

filepath="dbfs:/mnt/csvFiles/olist_orders_dataset.csv"
df6=spark.read.format("csv").option("header",True).load(filepath)

# COMMAND ----------

display(df6)

# COMMAND ----------

df6.createOrReplaceTempView("olist_order")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from olist_order

# COMMAND ----------

filepath="dbfs:/mnt/csvFiles/olist_customers_dataset.csv"
df1=spark.read.format("csv").option("header",True).load(filepath)
display(df1)
df1.createOrReplaceTempView("olist_customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from olist_customers

# COMMAND ----------

# MAGIC %md
# MAGIC # Top 10 state where maximum order has been placed

# COMMAND ----------

# MAGIC %sql
# MAGIC select t.state, t.no_of_Orders
# MAGIC from (select s.state, s.no_of_Orders, row_number() over(order by s.no_of_Orders desc) as rownumber 
# MAGIC       from (select c.customer_state as state, count(c.customer_state) as no_of_Orders
# MAGIC             from olist_customers as c join olist_order as o on c.customer_id = o.customer_id 
# MAGIC             group by c.customer_state
# MAGIC             order by count(c.customer_state) desc
# MAGIC             ) as s
# MAGIC       ) as t
# MAGIC where t.rownumber < 11 

# COMMAND ----------

DF_state = spark.sql("""
select t.state, t.no_of_Orders
from (select s.state, s.no_of_Orders, row_number() over(order by s.no_of_Orders desc) as rownumber 
      from (select c.customer_state as state, count(c.customer_state) as no_of_Orders
            from olist_customers as c join olist_order as o on c.customer_id = o.customer_id 
            group by c.customer_state
            order by count(c.customer_state) desc
            ) as s
      ) as t
where t.rownumber < 11 
""")

# COMMAND ----------

display(DF_state)

# COMMAND ----------

DF_state.write.format("parquet").save("/mnt/inputparq/State")

# COMMAND ----------

# MAGIC %md
# MAGIC # Number of order placed as per city for the specific state

# COMMAND ----------

# MAGIC %sql
# MAGIC select t.City, t.no_of_Orders
# MAGIC from (select s.City, s.no_of_Orders, row_number() over(order by s.no_of_Orders desc) as rownumber 
# MAGIC       from (select c.customer_city as City, count(c.customer_city) as no_of_Orders
# MAGIC             from olist_customers as c join olist_order as o on c.customer_id = o.customer_id 
# MAGIC             group by c.customer_city
# MAGIC             order by count(c.customer_city) desc
# MAGIC             ) as s
# MAGIC       ) as t

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from olist_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC select c.customer_state, c.customer_city as City, count(c.customer_city) as no_of_Orders
# MAGIC from olist_customers as c join olist_order as o on c.customer_id = o.customer_id 
# MAGIC group by c.customer_state,c.customer_city
# MAGIC order by count(c.customer_city) desc , customer_state 

# COMMAND ----------

# MAGIC %sql
# MAGIC select c.customer_state, t.City, t.no_of_Orders
# MAGIC from (select s.City, s.no_of_Orders, row_number() over(order by s.no_of_Orders desc) as rownumber 
# MAGIC       from (select c.customer_city as City, count(c.customer_city) as no_of_Orders
# MAGIC             from olist_customers as c join olist_order as o on c.customer_id = o.customer_id 
# MAGIC             group by c.customer_city
# MAGIC             order by count(c.customer_city) desc
# MAGIC             ) as s
# MAGIC       ) as t

# COMMAND ----------

DF_City = spark.sql("""
select t.City, t.no_of_Orders
from (select s.City, s.no_of_Orders, row_number() over(order by s.no_of_Orders desc) as rownumber 
      from (select c.customer_city as City, count(c.customer_city) as no_of_Orders
            from olist_customers as c join olist_order as o on c.customer_id = o.customer_id 
            group by c.customer_city
            order by count(c.customer_city) desc
            ) as s
      ) as t
""")

# COMMAND ----------

DF_City.write.format("parquet").save("/mnt/inputparq/City")

# COMMAND ----------

# MAGIC %md
# MAGIC # Number of orders placed on certain date 

# COMMAND ----------

# MAGIC %sql
# MAGIC select to_date(order_purchase_timestamp) as ordered_Date, count(to_date(order_purchase_timestamp)) as no_of_orders
# MAGIC from olist_order
# MAGIC group by to_date(order_purchase_timestamp)
# MAGIC order by count(to_date(order_purchase_timestamp)) desc

# COMMAND ----------

DF_Date = spark.sql("""
select to_date(order_purchase_timestamp) as ordered_Date, count(to_date(order_purchase_timestamp)) as no_of_orders
from olist_order
group by to_date(order_purchase_timestamp)
order by count(to_date(order_purchase_timestamp)) desc
""")

# COMMAND ----------

DF_Date.write.format("parquet").save("/mnt/inputparq/Date")

# COMMAND ----------

# MAGIC %md
# MAGIC # Count of orders as per order status

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(order_status) from olist_order

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_status , count(order_status) as status 
# MAGIC from olist_order
# MAGIC group by order_status

# COMMAND ----------

DF_status = spark.sql("""
select order_status , count(order_status) as status 
from olist_order
group by order_status
""")

# COMMAND ----------

DF_status.write.format("parquet").save("/mnt/inputparq/status")

# COMMAND ----------

# MAGIC %md
# MAGIC # Number of orders placed by customer

# COMMAND ----------

# MAGIC %sql
# MAGIC select lr.customer_id, Count(lr.customer_id)
# MAGIC from olist_order as lr
# MAGIC Group by lr.customer_id, lr.order_purchase_timestamp
# MAGIC order by Count(lr.customer_id) desc

# COMMAND ----------

DF_CustOrder = spark.sql ("""
select lr.customer_id, Count(lr.customer_id)
from olist_order as lr
Group by lr.customer_id, lr.order_purchase_timestamp
order by Count(lr.customer_id) desc
""")

# COMMAND ----------

DF_CustOrder.write.format("parquet").save("/mnt/inputparq/CustOrder")
