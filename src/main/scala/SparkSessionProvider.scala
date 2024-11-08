import org.apache.spark.sql.SparkSession

object SparkSessionProvider {
  lazy val spark: SparkSession = {
    val session = SparkSession
      .builder()
      .appName("Practice")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    session.sparkContext.setLogLevel("WARN")

    session
  }
}
