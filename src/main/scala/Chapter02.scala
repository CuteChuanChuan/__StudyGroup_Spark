import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Chapter02 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Chapter02")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    val flightData2015: DataFrame = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("src/main/resources/data/flight-data/csv/2015-summary.csv")
    
    // Idea: Use explain to see the physical plan (reading with 1 partition and sorting with 4 partitions)
    flightData2015.sort("count").explain
    
    
    // Idea: both SQL and DataFrame will be compiled to the same plan
    flightData2015.createOrReplaceTempView("flight_data_2015")
    
    val sqlWay = spark.sql(
      s"""
         |SELECT DEST_COUNTRY_NAME, count(1)
         |FROM flight_data_2015
         |GROUP BY DEST_COUNTRY_NAME
         |""".stripMargin)
    
    sqlWay.explain
    
    val dataFrameWay = flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .count()
    
    dataFrameWay.explain
    
    // Idea: Both SQL and DataFrame already have many functions like count, min, max, etc.
    flightData2015.select(max("count")).take(1).foreach(println)
    
    // Idea: Perform some complex operations
    val sumSQLWay = spark.sql(
      s"""
         |SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
         |FROM flight_data_2015
         |GROUP BY DEST_COUNTRY_NAME
         |ORDER BY destination_total DESC
         |LIMIT 5
         |""".stripMargin)
    
    val sumDataFrameWay1 = flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .agg(sum("count").as("destination_total"))
      .sort(col("destination_total").desc)
      .limit(5)
    
    val sumDataFrameWay2 = flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "destination_total")
      .sort(desc("destination_total"))
      .limit(5)
    
    sumSQLWay.explain
    sumDataFrameWay1.explain
    sumDataFrameWay2.explain
    
    sumSQLWay.show()
    sumDataFrameWay1.show()
    sumDataFrameWay2.show()
    
    spark.stop()
  }
}
