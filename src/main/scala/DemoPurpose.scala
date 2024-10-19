import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DemoPurpose {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.spark
    
    val data = Seq(
      ("Alice", 25),
      ("Bob", 30),
      ("Charlie", 35)
    )
    
    val df: DataFrame = spark.createDataFrame(data).toDF("name", "age")
    df.show()
    
    spark.stop()
  }
}
