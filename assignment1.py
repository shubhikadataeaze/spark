#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, trim, lower

spark = SparkSession.builder \
    .appName("Assignments") \
    .master("local[*]") \
    .getOrCreate()


# In[2]:


cons = spark.read.parquet("path/to/consumerInternet.parquet")
start = spark.read.option("header", True).csv("path/to/startups.csv")
combined = cons.unionByName(start)
combined.createOrReplaceTempView("all_data")


# In[4]:


startups = spark.read.option("header", True).csv("C:/Users/admin/Desktop/spark_assignment/startup.csv")
consumer = spark.read.parquet("C:/Users/admin/Desktop/spark_assignment/consumerInternet.parquet")

combined = consumer.unionByName(startups)
combined.createOrReplaceTempView("startups")


# In[5]:


spark.sql("SELECT COUNT(*) AS pune_startups FROM startups WHERE City = 'Pune'").show()


# In[6]:


spark.sql("""
SELECT COUNT(*) AS seed_angel_pune 
FROM startups 
WHERE City = 'Pune' AND InvestmentnType IN ('Seed Funding', 'Angel Funding')
""").show()


# In[7]:


from pyspark.sql.functions import regexp_replace

combined = combined.withColumn("clean_amount", regexp_replace("Amount_in_USD", ",", ""))
combined = combined.withColumn("clean_amount", combined["clean_amount"].cast("double"))
combined.createOrReplaceTempView("startups")

spark.sql("""
SELECT SUM(clean_amount) AS total_pune_funding
FROM startups WHERE City = 'Pune'
""").show()


# In[8]:


spark.sql("""
SELECT Industry_Vertical, COUNT(*) AS count
FROM startups
GROUP BY Industry_Vertical
ORDER BY count DESC
LIMIT 5
""").show()


# In[9]:


from pyspark.sql.functions import year, to_date, col, row_number
from pyspark.sql.window import Window

df = combined.withColumn("year", year(to_date("Date", "dd/MM/yyyy")))
df.createOrReplaceTempView("startups")

top_investors = spark.sql("""
    SELECT year, Investors_Name, SUM(clean_amount) AS total
    FROM startups
    GROUP BY year, Investors_Name
""")

windowSpec = Window.partitionBy("year").orderBy(col("total").desc())
ranked = top_investors.withColumn("rank", row_number().over(windowSpec)).filter("rank = 1")
ranked.select("year", "Investors_Name", "total").show()


# In[10]:


combined.select("Date").show(10, truncate=False)


# In[11]:


from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

top_investors = df.groupBy("year", "Investors_Name").sum("clean_amount").withColumnRenamed("sum(clean_amount)", "total")

windowSpec = Window.partitionBy("year").orderBy(col("total").desc())
ranked = top_investors.withColumn("rank", row_number().over(windowSpec)).filter("rank = 1")

ranked.select("year", "Investors_Name", "total").show()


# In[12]:


from pyspark.sql.functions import to_date, year, col

df = combined.withColumn("ParsedDate", to_date("Date", "dd/MM/yyyy"))
df = df.withColumn("year", year(col("ParsedDate")))
df.select("Date", "ParsedDate", "year").show(5)


# In[13]:


from pyspark.sql.functions import regexp_replace

df = df.withColumn("clean_amount", regexp_replace("Amount_in_USD", ",", "").cast("double"))
df.select("Amount_in_USD", "clean_amount").show(10)


# In[14]:


from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

top_investors = df.groupBy("year", "Investors_Name").sum("clean_amount").withColumnRenamed("sum(clean_amount)", "total")

windowSpec = Window.partitionBy("year").orderBy(col("total").desc())
ranked = top_investors.withColumn("rank", row_number().over(windowSpec)).filter("rank = 1")

ranked.select("year", "Investors_Name", "total").show()


# In[15]:


from pyspark.sql.functions import row_number, col
from pyspark.sql.window import Window

top_investors = df.groupBy("year", "Investors_Name")\
    .sum("clean_amount")\
    .withColumnRenamed("sum(clean_amount)", "total")\
    .filter("total is not null")

windowSpec = Window.partitionBy("year").orderBy(col("total").desc())
ranked = top_investors.withColumn("rank", row_number().over(windowSpec)).filter("rank = 1")

ranked.show(10, truncate=False)


# In[16]:


from pyspark.sql.functions import col, length

# Only accept rows with date strings of length 10 and year >= 2000
valid_df = df.filter(
    (length("Date") == 10) &
    (col("Date").substr(-4, 4).cast("int") >= 2000)
)


# In[17]:


from pyspark.sql.functions import substring

# Add year column
df_with_year = valid_df.withColumn("year", substring(col("Date"), -4, 4).cast("int"))


# In[18]:


from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# Group and rank
top_investors = df_with_year.groupBy("year", "Investors_Name")\
    .sum("clean_amount")\
    .withColumnRenamed("sum(clean_amount)", "total")\
    .filter("total is not null")

windowSpec = Window.partitionBy("year").orderBy(col("total").desc())
ranked = top_investors.withColumn("rank", row_number().over(windowSpec)).filter("rank = 1")

# Show results
ranked.select("year", "Investors_Name", "total").show(10, truncate=False)


# In[19]:


from pyspark.sql.functions import row_number, col
from pyspark.sql.window import Window

# Assume df has clean_amount column & City
win_city = Window.partitionBy("City").orderBy(col("clean_amount").desc())

top_startup_city = df.withColumn("rk", row_number().over(win_city)) \
                     .filter("rk = 1") \
                     .select("City", "Startup_Name", "clean_amount")

top_startup_city.show(truncate=False)


# In[20]:


# Count startups per SubVertical per Year
counts = df.groupBy("year", "SubVertical").count()

# Use window to compute difference between consecutive years
from pyspark.sql.functions import lag
from pyspark.sql.window import Window

win = Window.partitionBy("SubVertical").orderBy("year")
growth = counts.withColumn("prev_count", lag("count").over(win)) \
               .withColumn("growth", col("count") - col("prev_count"))

# Find max growth value
max_growth = growth.groupBy("SubVertical") \
                   .agg({"growth": "max"}) \
                   .withColumnRenamed("max(growth)", "max_growth") \
                   .orderBy(col("max_growth").desc())

max_growth.show(10, truncate=False)


# In[21]:


df.filter("year IS NULL OR SubVertical IS NULL").show()


# In[22]:


from pyspark.sql.functions import col

# Remove rows with incorrectly formatted years (length != 4)
df_valid = df.filter("length(SUBSTRING_INDEX(Date, '/', -1)) = 4")


# In[23]:


from pyspark.sql.functions import substring_index

df_with_year = df_valid.withColumn("year", substring_index("Date", "/", -1))


# In[24]:


from pyspark.sql.functions import lag, col
from pyspark.sql.window import Window

# Count startups per year per SubVertical
subv_counts = df_with_year.groupBy("year", "SubVertical").count()

# Define window
win = Window.partitionBy("SubVertical").orderBy("year")

# Calculate growth
growth = subv_counts.withColumn("prev_count", lag("count").over(win)) \
                    .withColumn("growth", col("count") - col("prev_count"))

# Max growth per SubVertical
max_growth = growth.groupBy("SubVertical") \
                   .agg({"growth": "max"}) \
                   .withColumnRenamed("max(growth)", "max_growth") \
                   .orderBy(col("max_growth").desc())

max_growth.show(10, truncate=False)


# In[ ]:




