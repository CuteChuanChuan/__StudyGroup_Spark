import org.apache.spark.sql.SparkSession

object Chapter10 {
  val spark: SparkSession = SparkSessionProvider.spark
  
  def main(args: Array[String]): Unit = {
    
    // DataFrame -> SQL
    spark.read
      .json("src/main/resources/data/flight-data/json/2015-summary.json")
      .createOrReplaceTempView("view1")
    
    // SQL -> DataFrame
    val resultCnt: Long = spark.sql(
        s"""
           |SELECT DEST_COUNTRY_NAME, sum(count) as cnt_sum FROM view1 GROUP BY DEST_COUNTRY_NAME
           |""".stripMargin)
      .where("DEST_COUNTRY_NAME like 'S%'")
      .where("cnt_sum > 10")
      .count()
    
    println(resultCnt)
  }
}
