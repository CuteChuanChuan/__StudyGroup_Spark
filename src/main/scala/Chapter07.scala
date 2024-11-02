import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object Chapter07 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.spark
    
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    
    val df = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("src/main/resources/data/retail-data/all/*.csv")
    
    df.cache
    df.createOrReplaceTempView("dfTable")
    df.printSchema
    
    // count & count distinct & approx_count_distinct
    df.select(count("StockCode")).show
    df.select(countDistinct("StockCode")).show
    df.select(approx_count_distinct("StockCode", 0.1)).show
    
    // first and last
    df.select(first("StockCode"), last("StockCode")).show
    
    // min and max
    df.select(min("Quantity"), max("Quantity")).show
    
    // sum and sum distinct
    df.select(sum("Quantity")).show
    df.select(sum_distinct(col("Quantity"))).show
    
    // avg
    df.select(
      count("Quantity") as "Total_Quantity",
      sum("Quantity") as "Total_Sales",
      avg("Quantity") as "Avg_Sales",
      expr("mean(Quantity)") as "Mean_Sales"
    ).selectExpr(
      "Total_Sales / Total_Quantity",
      "Avg_Sales",
      "Mean_Sales"
    ).show(5)
    
    // variance and std dev
    df.select(
      var_pop("Quantity"),
      var_samp("Quantity"),
      stddev_pop("Quantity"),
      stddev_samp("Quantity")
    ).show
    
    // skewness and kurtosis
    df.select(skewness("Quantity"), kurtosis("Quantity")).show
    
    // covariance and correlation
    df.select(
      corr("InvoiceNo", "Quantity"),
      covar_samp("InvoiceNo", "Quantity"),
      covar_pop("InvoiceNo", "Quantity")
    ).show
    
    // aggregate complex types
    df.agg(
      collect_set("Country"),
      collect_list("Country")
    ).show
    
    // group by
    df.groupBy("InvoiceNo", "CustomerId").count().show
    df.groupBy("InvoiceNo").agg(
      count("Quantity").as("total_quantity"),
      expr("count(Quantity)")
    ).show
    df.groupBy("InvoiceNo").agg("Quantity" -> "avg", "Quantity" -> "sum", "Quantity" -> "stddev_pop").show
    
    // window functions
    val dfWithDate = df
      .withColumn("date", to_timestamp(col("InvoiceDate"), "MM/dd/yyyy HH:mm"))
    dfWithDate.createOrReplaceTempView("dfWithDate")
    val windowSpec = Window
      .partitionBy("CustomerId", "date")
      .orderBy(col("Quantity").desc)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)
    val purchaseDenseRank = dense_rank().over(windowSpec)
    val purchaseRank = rank().over(windowSpec)
    
    dfWithDate.where("CustomerId is not null").orderBy("CustomerId")
      .select(
        col("CustomerId"),
        col("date"),
        col("Quantity"),
        purchaseRank.alias("quantityRank"),
        purchaseDenseRank.alias("quantityDenseRank"),
        maxPurchaseQuantity.alias("maxQuantity")
      ).show
    
    // grouping sets
    val dfNoNull = dfWithDate.drop()
    dfNoNull.createOrReplaceTempView("dfNotNull")
    
    val groupingSetSQL: String =
      """
        |SELECT CustomerId, stockCode, sum(Quantity) FROM dfNotNull
        |GROUP BY CustomerId, stockCode GROUPING SETS ((CustomerId, stockCode))
        |ORDER BY CustomerId DESC, stockCode DESC
        |""".stripMargin
    spark.sql(groupingSetSQL).show
    
    // rollup
    val rolledUpDf = dfNoNull.rollup("Date", "Country").agg(sum("Quantity"))
      .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
      .orderBy("Date")
    rolledUpDf.show
    
    // cube
    val cubeDf = dfNoNull.cube("Date", "Country").agg(sum("Quantity"))
      .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
      .orderBy("Date")
    cubeDf.show
    
    // grouping metadata
    dfNoNull.cube("customerId", "stockCode").agg(grouping_id(), sum("Quantity"))
      .orderBy(expr("grouping_id()").desc)
      .show
    
    // pivot
    val pivoted = dfWithDate.groupBy("date").pivot("Country").sum()
    pivoted.show()
    
    pivoted.where("date > '2011-12-05'").select("date", "USA_sum(Quantity)").show
    
    
    
    spark.stop
  }
}
