import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)

object Chapter03 {
  
  
  def main(args: Array[String]): Unit = {
    
    
    val spark = SparkSession.builder
      .appName("Chapter03")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    
    import spark.implicits._
    
    // Idea: Demonstrate DataSet
    val flightData2015Parquet = spark
      .read
      .parquet("src/main/resources/data/flight-data/parquet/2010-summary.parquet")
    
    val flightData2015DataSet = flightData2015Parquet.as[Flight]
    
    flightData2015DataSet
      .filter(flightRow => flightRow.ORIGIN_COUNTRY_NAME != "Canada")
      .show
    
    flightData2015DataSet
      .filter(flightRow => flightRow.ORIGIN_COUNTRY_NAME != "Canada")
      .map(eachRow => Flight(eachRow.DEST_COUNTRY_NAME, eachRow.ORIGIN_COUNTRY_NAME, eachRow.count + 5))
      .show
    
    // Idea: Demonstrate Streaming
    val staticDataFrame = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/retail-data/by-day/*.csv")
    
    val staticSchema = staticDataFrame.schema
    
    staticDataFrame
      .filter("CustomerID is not null")
      .selectExpr("CustomerID", "(UnitPrice * Quantity) as total_cost", "InvoiceDate")
      .groupBy(col("CustomerID"), window(col("InvoiceDate"), "1 day"))
      .sum("total_cost")
      .withColumnRenamed("sum(total_cost)", "total_cost")
      .sort("CustomerID", "window")
      .show(50)
    
    val streamingDataFrame = spark
      .readStream
      .schema(staticSchema)
      .option("header", "true")
      .option("maxFilesPerTrigger", "1")
      .format("csv")
      .csv("src/main/resources/data/retail-data/by-day/*.csv")
    
    streamingDataFrame.isStreaming
    
    val purchaseByCustomerPerHour = streamingDataFrame
      .selectExpr("CustomerID", "(UnitPrice * Quantity) as total_cost", "InvoiceDate")
      .groupBy($"CustomerID", window($"InvoiceDate", "1 day"))
      .sum("total_cost")
    
    val query = purchaseByCustomerPerHour
      .writeStream
      .format("console")
      .queryName("customer_purchases")
      .outputMode("complete")
      .start()
    
    query.awaitTermination()
    spark.stop()
  }
}
