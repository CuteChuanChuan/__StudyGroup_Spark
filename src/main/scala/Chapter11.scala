import org.apache.spark.sql.DataFrame

case class FlightInfo(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)
case class FlightMetadata(count: BigInt, randomData: BigInt)

object Chapter11 {
  
  private def originIsDest(flightRow: FlightInfo): Boolean = {
    flightRow.ORIGIN_COUNTRY_NAME == flightRow.DEST_COUNTRY_NAME
  }
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.spark
    
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    
    import spark.implicits._
    
    val flightData2015: DataFrame = spark
      .read
      .parquet("src/main/resources/data/flight-data/parquet/2010-summary.parquet")
    
    val flights = flightData2015.as[FlightInfo]
    
    flightData2015.printSchema
    flights.printSchema
    flights.show(numRows = 2)
    
    // filtering
    val flightSample = flights.filter(flightRow => originIsDest(flightRow)).first
    println(flightSample)
    
    // mapping
    val destinations = flights.map(row => row.DEST_COUNTRY_NAME)
    destinations.show(numRows = 5)
    
    //joining
    
    val flightMeta = spark.range(500).map(x => (x, scala.util.Random.nextLong))
      .withColumnRenamed("_1", "count")
      .withColumnRenamed("_2", "randomData")
      .as[FlightMetadata]
    
    val flights2 = flights
      .joinWith(flightMeta, flights.col("count") === flightMeta.col("count"))
    
    // complex types from joinWith
    flights2.show(2)
    flights2.selectExpr("_1.DEST_COUNTRY_NAME", "_2.randomData").show(2)
    
  }
}
