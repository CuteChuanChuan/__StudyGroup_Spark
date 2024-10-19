import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

object Chapter05 {
  
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSessionProvider.spark
    
    // Create a DataFrame abd fetch schema (schema-on-read)
    val df = spark.read
      .json("src/main/resources/data/flight-data/json/2015-summary.json")
    df.schema.foreach(println)
    df.printSchema
    
    // Define schema on our own
    val manualSchema = StructType(Array(
      StructField("DEST_COUNTRY_NAME", StringType, nullable = true),
      StructField("ORIGIN_COUNTRY_NAME", StringType, nullable = true),
      StructField("count", LongType, nullable = false, Metadata.fromJson("""{"hello":"world"}"""))
    ))
    
    val df2 = spark.read
      .schema(manualSchema)
      .json("src/main/resources/data/flight-data/json/2015-summary.json")
    df2.printSchema
    df2.columns.foreach(println)
    
    // Columns are expressions
    df.selectExpr("*", "count + 5").show()
    df.withColumn("new_count", col("count") + 5).show()
    
    // Create Rows and access elements
    val aRow = Row("Hello", null, 1, false)
    println(aRow(0))
    println(aRow(0).asInstanceOf[String])
    println(aRow.getString(0))
    println(aRow.getInt(2))
    
    // select and selectExpr
    import spark.implicits._
    df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)
    df.select(
      df.col("DEST_COUNTRY_NAME"),
      col("DEST_COUNTRY_NAME"),
      column("DEST_COUNTRY_NAME"),
      expr("DEST_COUNTRY_NAME"),
      $"DEST_COUNTRY_NAME",
    ).show(2)
    
    // Do not mix column objects and string names
    df.select(
      "DEST_COUNTRY_NAME"
    ).show(2)
    
    // expr is more flexible reference than col
    df.select(expr("DEST_COUNTRY_NAME AS destination")).show((2))
    df.select(expr("DEST_COUNTRY_NAME AS destination").alias("DEST_COUNTRY_NAME")).show((2))
    
    // selectExpr is a simple way to build up complex expressions
    df.selectExpr(
      "*",
      "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) AS withinCountry"
    ) show (2)
    
    df.selectExpr(
      "avg(count)", "count(distinct(DEST_COUNTRY_NAME))"
    ).show(2)
    
    // Literal values: converting values from a given programming language to one that Spark can understand
    df.select(expr("*"), lit(1).as("One")).show(2)
    df.selectExpr("*", "1 as One").show(2)
    
    // withColumn
    df.withColumn("Destination", col("DEST_COUNTRY_NAME")).show(2)
    
    // Rename columns
    df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").show(2)
    
    val dfWithLongColName = df.withColumn("This Long Column-Name", expr("ORIGIN_COUNTRY_NAME"))
    dfWithLongColName.selectExpr(
      "`This Long Column-Name`",
      "`This Long Column-Name` as `new col`"
    ).show(2)
    
    // Remove columns
    dfWithLongColName.drop("new col").show(2)
    
    // Change column type
    df.withColumn("casted", col("count").cast("Int")).printSchema
    
    // Filter rows
    df.filter(col("count") < 2).show(2)
    df.filter(expr("count < 2")).show(2)
    df.where(col("count") < 2).show(2)
    df.where(expr("count < 2")).show(2)
    
    // Chain filters: Just chan the filter operations and let Spark handle the rest
    df.where(col("count") < 2).filter(expr("ORIGIN_COUNTRY_NAME = 'Croatia'")).show(2)
    
    // Distinct Rows
    val distinctOriginCountries = df.selectExpr("ORIGIN_COUNTRY_NAME").distinct.count
    println(distinctOriginCountries)
    
    // Sampling
    val seed = 5
    val withReplacement = false
    val fraction = 0.5
    df.sample(withReplacement, fraction, seed).show(2)
    
    // Random splits
    val dataFrames = df.randomSplit(Array(0.25, 0.75), seed)
    dataFrames(0).count > dataFrames(1).count
    
    // Union
    val schema = df.schema
    val newRows = Seq(
      Row("New Country", "Other Country", 5L),
      Row("New Country 2", "Other Country 3", 1L)
    )
    
    val originalCounts = df.count
    val parallelizedDf = spark.sparkContext.parallelize(newRows)
    val newDf = spark.createDataFrame(parallelizedDf, schema)
    val unionCounts = df.union(newDf).count
    println(unionCounts - originalCounts)
    
    df.union(newDf)
      .where("count = 1")
      .where($"ORIGIN_COUNTRY_NAME" =!= "United States") // =!=: compare to the evaluated value
      .show
    
    // Sort
    df.sort("count").show(5)
    df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
    df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)
    df.orderBy(expr("count desc")).show(5)
    df.sort(desc("count")).show(5)
    df.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).show(5)
    
    // For optimization purpose, use sortWithinPartitions before some transformation
    spark.read.json("src/main/resources/data/flight-data/json/2015-summary.json").sortWithinPartitions("count").explain
    
    // Partition & coalesce
    df.rdd.getNumPartitions
    df.repartition(2)
    df.repartition(col("DEST_COUNTRY_NAME"))
    df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(1)
    
    
    spark.stop()
  }
  
}
