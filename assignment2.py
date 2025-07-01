from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Startup Assignment") \
    .getOrCreate()

# Load the CSV file
df = spark.read.option("header", True).csv("file:///C:/Users/admin/Desktop/spark_assignment/startup.csv")

df = df.withColumn("startup_name_processed", regexp_replace(col("Startup_Name"), " ", ""))

# Transformation 2: Make 'City' lowercase
df = df.withColumn("City", lower(col("City")))

# Show and print schema
df.show(5)
df.printSchema()

# Write to MySQL
df.write \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/startupdb") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "startups") \
    .option("user", "root") \
    .option("password", "password") \
    .mode("append") \
    .save()
