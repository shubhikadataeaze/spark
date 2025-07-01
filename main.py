from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("SparkAssignment") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .getOrCreate()

# Suppress logs to avoid clutter
spark.sparkContext.setLogLevel("ERROR")

# Read data
consumer_df = spark.read.parquet("consumerInternet.parquet")
startup_df = spark.read.option("header", True).csv("startup.csv")

print("Q1. Startups in Pune:")
q1 = startup_df.filter(startup_df["City"] == "Pune")
q1.show(truncate=False)

print("Q2. Startups funded by seed/angel:")
q2 = startup_df.filter(startup_df["Investors_Name"].like("%Sequoia Capital%"))

q2.show(truncate=False)

print("Q3. Startup count by industry vertical:")
#q3 = startup_df.groupBy("IndustryVertical").count().orderBy("count", ascending=False)
q3 = startup_df.groupBy("Industry_Vertical").count().orderBy("count", ascending=False)

q3.show(truncate=False)

print("Q4. Top 10 most funded startups (by AmountInUSD):")
from pyspark.sql.functions import col
q4 = startup_df.withColumn("Amount_In_USD", col("Amount_In_USD").cast("double")) \
               .orderBy(col("Amount_In_USD").desc_nulls_last()) \
               .select("Startup_Name", "Amount_In_USD") \
               .limit(10)
q4.show(truncate=False)

print("Q5. Most active investor:")
from pyspark.sql.functions import explode, split
q5 = startup_df.withColumn("Investor", explode(split("Investors_Name", ","))) \
               .groupBy("Investor").count() \
               .orderBy("count", ascending=False)
q5.show(5, truncate=False)
