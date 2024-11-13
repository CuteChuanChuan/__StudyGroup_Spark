import org.apache.spark.sql.functions._

object Chapter22 {
  def main(args: Array[String]): Unit = {
    val spark    = SparkSessionProvider.spark
    val staticDf = spark.read.json(
      "/Users/raymond/Downloads/Spark-The-Definitive-Guide-master/data/activity-data")

    val streamingDf = spark.readStream
      .schema(staticDf.schema)
      .option("maxFilesPerTrigger", 10)
      .json("/Users/raymond/Downloads/Spark-The-Definitive-Guide-master/data/activity-data")

    streamingDf.printSchema

    val withEventTime = streamingDf.selectExpr(
      "*",
      "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time"
    )

    // create a window not overlapping for every 10 minutes
    withEventTime
      .groupBy(window(col("event_time"), "10 minutes"))
      .count()
      .writeStream
      .queryName("event_per_window")
      .format("memory")
      .outputMode("complete")
      .start()

    spark.sql("select * from event_per_window").printSchema()

    // create a window overlapping for every 5 minutes and each window contains 10 minutes
    withEventTime
      .groupBy(window(col("event_time"), "10 minutes", "5 minutes"))
      .count()
      .writeStream
      .queryName("event_per_window2")
      .format("memory")
      .outputMode("complete")
      .start()
    spark.sql("select * from event_per_window2").printSchema()

    withEventTime
      .withWatermark("event_time", "1 hour")
      .groupBy(window(col("event_time"), "10 minutes", "5 minutes"))
      .count()
      .writeStream
      .queryName("event_per_window3")
      .format("memory")
      .outputMode("complete")
      .start()

    spark.sql("select * from event_per_window3").printSchema()

    withEventTime
      .withWatermark("event_time", "1 hour")
      .dropDuplicates("User", "event_time")
      .groupBy("User")
      .count()
      .writeStream
      .queryName("event_per_window4")
      .format("memory")
      .outputMode("complete")
      .start()

    spark.sql("select * from event_per_window4").printSchema()
  }
}
