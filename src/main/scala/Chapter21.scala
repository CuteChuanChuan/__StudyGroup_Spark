import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.streaming.Trigger

object Chapter21 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.spark

    spark.conf.set("spark.sql.shuffle.partitions", "4")
    import spark.implicits._

    val staticDf       = spark.read.json(
      "/Users/raymond/Downloads/Spark-The-Definitive-Guide-master/data/activity-data")
    val staticDfSchema = staticDf.schema
    staticDf.printSchema()

    val streamingDf = spark.readStream
      .schema(staticDfSchema)
      .option("maxFilesPerTrigger", 1)
      .json("/Users/raymond/Downloads/Spark-The-Definitive-Guide-master/data/activity-data")

    val activityCounts = streamingDf.groupBy("gt").count()
//    val activityQuery  = activityCounts.writeStream
//      .queryName("activity_counts")
//      .format("memory")
//      .outputMode("complete")
//      .start()
//
//    for (i <- 1 to 10) {
//      spark.sql("select * from activity_counts").show
//      Thread.sleep(1000)
//    }
//
//    activityQuery.awaitTermination()
//
//    val simpleTransform = streamingDf
//      .withColumn("stairs", expr("gt like '%stairs%'"))
//      .where("stairs")
//      .where("gt is not null")
//      .select("gt", "model", "arrival_time", "creation_time")
//      .writeStream
//      .queryName("simple_transform")
//      .format("memory")
//      .outputMode("append")
//      .start()
//    simpleTransform.awaitTermination()

//    val deviceModelStats = streamingDf
//      .cube("gt", "model")
//      .avg()
//      .drop("avg(Arrival_Time)")
//      .drop("avg(Creation_Time)")
//      .drop("avg(Index)")
//      .writeStream
//      .queryName("device_counts")
//      .format("memory")
//      .outputMode("complete")
//      .start()
//    for (i <- 1 to 10) {
//      spark.sql("select * from device_counts").show
//      Thread.sleep(1000)
//    }
//    deviceModelStats.awaitTermination()

    val historicalAgg    = staticDf.groupBy("gt", "model").avg()
    val deviceModelStats = streamingDf
      .drop("Arrival_Time", "Creation_Time", "Index")
      .cube("gt", "model")
      .avg()
      .join(historicalAgg, Seq("gt", "model"))
      .writeStream
      .queryName("device_counts")
      .format("memory")
      .outputMode("complete")
      .start()
    for (i <- 1 to 10) {
      spark.sql("select * from device_counts").show
      Thread.sleep(1000)
    }
    deviceModelStats.awaitTermination()

    val ds1 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1")
      .load()
    // Subscribe to multiple topics
    val ds2 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1,topic2")
      .load()
    // Subscribe to a pattern of topics
    val ds3 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribePattern", "topic.*")
      .load()

    ds1
      .selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("checkpointLocation", "/to/HDFS-compatible/dir")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .start()
    ds1
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("checkpointLocation", "/to/HDFS-compatible/dir")
      .option("topic", "topic1")
      .start()

    activityCounts.writeStream
      .trigger(Trigger.ProcessingTime("100 seconds"))
      .format("console")
      .outputMode("complete")
      .start()
    activityCounts.writeStream
      .trigger(Trigger.AvailableNow())
      .format("console")
      .outputMode("complete")
      .start()
  }
}
