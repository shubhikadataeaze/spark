import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark SQL Assignment")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val consumerDF = spark.read.parquet("consumerInternet.parquet")
    val startupDF = spark.read.option("header", "true").csv("startup.csv")

    consumerDF.createOrReplaceTempView("consumer")
    startupDF.createOrReplaceTempView("startup")

    println("----- Q1: Startups in Pune -----")
    val q1 = spark.sql("""
      SELECT COUNT(*) AS PuneStartups
      FROM startup
      WHERE lower(City) LIKE '%pune%'
    """)
    q1.show()

    println("----- Q2: Pune startups with Seed/Angel Funding -----")
    val q2 = spark.sql("""
      SELECT COUNT(*) AS PuneSeedAngel
      FROM startup
      WHERE lower(City) LIKE '%pune%' 
        AND lower(InvestmentnType) IN ('seed funding', 'angel funding')
    """)
    q2.show()

    println("----- Q3: Total amount raised by Pune startups -----")
    val cleanedDF = startupDF.withColumn("AmountRaisedCleaned",
      regexp_replace(col("Amount_in_USD"), "[^0-9]", "").cast("long"))
    cleanedDF.createOrReplaceTempView("cleanStartup")

    val q3 = spark.sql("""
      SELECT SUM(AmountRaisedCleaned) AS TotalPuneAmount
      FROM cleanStartup
      WHERE lower(City) LIKE '%pune%'
    """)
    q3.show()

    println("----- Q4: Top 5 Industry Verticals by startup count -----")
    val q4 = spark.sql("""
      SELECT Industry_Vertical, COUNT(*) AS Count
      FROM startup
      GROUP BY Industry_Vertical
      ORDER BY Count DESC
      LIMIT 5
    """)
    q4.show()

    println("----- Q5: Top investor by amount for each year -----")
    val startupWithYear = cleanedDF.withColumn("Year", split(col("Date"), "/").getItem(2))
    startupWithYear.createOrReplaceTempView("startupWithYear")

    val q5 = spark.sql("""
      SELECT Year, Investors_Name, MAX(AmountRaisedCleaned) AS MaxInvestment
      FROM startupWithYear
      GROUP BY Year, Investors_Name
      ORDER BY Year ASC, MaxInvestment DESC
    """)
    q5.show()

    spark.stop()
  }
}
