import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Chapter06 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.spark

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/retail-data/by-day/2010-12-01.csv")

    df.printSchema
    df.createOrReplaceTempView("dfTable")

    // Lit: convert a type in other language to corresponding Spark representation
    df.select(lit(5), lit("five"), lit(5.0)).printSchema

    // Booleans: foundations for filtering
    df.where(col("InvoiceNo").notEqual(536365))
      .select("InvoiceNo", "StockCode", "Description")
      .show(5)

    df.where(col("InvoiceNo").equalTo(536365))
      .select("InvoiceNo", "StockCode", "Description")
      .show(5, truncate = false)

    df.where(col("InvoiceNo") =!= 536365)
      .select("InvoiceNo", "StockCode", "Description")
      .show(5, truncate = false)

    df.where(col("InvoiceNo") === 536365)
      .select("InvoiceNo", "StockCode", "Description")
      .show(5, truncate = false)

    df.where("InvoiceNo <> 536365")
      .select("InvoiceNo", "StockCode", "Description")
      .show(5, truncate = false)

    df.where("InvoiceNo = 536365")
      .select("InvoiceNo", "StockCode", "Description")
      .show(5, truncate = false)

    // Chain multiple filter conditions (no need to add 'and' or 'or')
    df.withColumn("isExpensive", not(col("UnitPrice").leq(250)))
      .filter("isExpensive")
      .select("Description", "UnitPrice")
      .show(5, truncate = false)

    df.withColumn("isExpensive", expr("UnitPrice > 250"))
      .filter("isExpensive")
      .select("Description", "UnitPrice")
      .show(5, truncate = false)

    df.withColumn("isExpensive", expr("Not UnitPrice <= 250"))
      .filter("isExpensive")
      .select("Description", "UnitPrice")
      .show(5, truncate = false)

    // treat differently if working with nulls
    df.where(col("Description").eqNullSafe("hello")).show

    // power function
    val fabricateQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
    df.select(
      expr("CustomerId"),
      col("Quantity"),
      column("UnitPrice"),
      fabricateQuantity.alias("realQuantity"))
      .show(2)
    df.selectExpr("CustomerId", "POWER(Quantity * UnitPrice, 2.0) + 5 as realQuantity")
      .show(2)

    // round function
    df.select(round(lit(2.5)), bround(lit(2.5))).show(2)

    // correlation
    df.stat.corr("Quantity", "UnitPrice")
    df.select(corr("Quantity", "UnitPrice")).show

    // describe
    df.describe().show
    df.stat.crosstab("StockCode", "Quantity").show

    // add unique id
    df.select(monotonically_increasing_id()).show(2)

    // uppercase and lowercase
    df.select(initcap(col("Description"))).show(2, truncate = false)
    df.select(col("Description"), lower(col("Description")), upper(col("Description")))
      .show(2, truncate = false)

    // trim and pad
    df.select(
      ltrim(lit(" HELLO ")).as("ltrim"),
      rtrim(lit(" HELLO ")).as("rtrim"),
      trim(lit(" HELLO ")).as("trim"),
      lpad(lit("HELLO "), 2, " ").as("lpad"),
      rpad(lit("HELLO "), 6, " ").as("rpad")
    ).show(1, truncate = false)

    //regex
    val simpleColors = Seq("black", "white", "red", "green", "blue")
    val regexString  = simpleColors.map(_.toUpperCase).mkString("|")
    df.select(
      regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean"),
      col("Description")
    ).show(3, truncate = false)

    // replace
    df.select(translate(col("Description"), "LEWT", "1347"), col("Description"))
      .show(3, truncate = false)

    // extract
    val regexString2 = simpleColors.map(_.toUpperCase).mkString("(", "|", ")")
    println(regexString2)

    df.select(
      col("Description"),
      regexp_extract(col("Description"), regexString2, 1).alias("color_clean"))
      .show(3, truncate = false)

    // check existence
    val containsBlack = col("Description").contains("BLACK")
    val containsWhite = col("Description").contains("WHITE")
    df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
      .where("hasSimpleColor")
      .select("Description")
      .show(3, truncate = false)

    val selectedColumns = simpleColors.map { color =>
      col("Description").contains(color.toUpperCase).alias(s"is_$color")
    }
      :+ expr("*")

    df.select(selectedColumns: _*)
      .where(col("is_white").or(col("is_red")))
      .select("Description")
      .show(3, truncate = false)

    val dateDf = spark
      .range(10)
      .withColumn("today", current_date())
      .withColumn("now", current_timestamp())
    dateDf.createOrReplaceTempView("dateTable")
    dateDf.printSchema

    dateDf.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)
    dateDf
      .withColumn("week_ago", date_sub(col("today"), 7))
      .select("today", "week_ago")
      .show(1)

    dateDf
      .select(to_date(lit("2016-01-01")).alias("start"), to_date(lit("2017-05-22")).alias("end"))
      .select(months_between(col("end"), col("start")))
      .show(1)

    val dateFormat  = "yyyy-dd-MM"
    val cleanDateDf = spark
      .range(1)
      .select(
        to_date(lit("2016-12-11"), dateFormat).alias("start"),
        to_date(lit("2016-20-11"), dateFormat).alias("end")
      )
    cleanDateDf.show()
    cleanDateDf.createOrReplaceTempView("dateTable2")
    cleanDateDf.select(to_timestamp(col("start")), to_date(col("end"))).show

    // coalesce: returns the first non-null value
    df.select(coalesce(col("Description"), col("InvoiceNo"))).show
    df.na.drop
    df.na.fill("Convert all nulls to this")

    val fillColValues = Map("StockCode" -> 5, "Description" -> "No Value")
    df.na.fill(fillColValues)

    df.na.replace("Description", Map("" -> "UNKNOWN"))

    // complex types - struct
    df.select(struct(col("Description"), col("InvoiceNo")).alias("complex"))
      .selectExpr("complex")
      .show(2, truncate = false)

    df.select(struct(col("Description"), col("InvoiceNo")).alias("complex"))
      .selectExpr("complex.Description", "complex.InvoiceNo")
      .show(2, truncate = false)

    df.select(struct(col("Description"), col("InvoiceNo")).alias("complex"))
      .select(col("complex").getField("InvoiceNo"))
      .show(2, truncate = false)

    // complex types - array
    df.select(split(col("Description"), " ").alias("words"))
      .selectExpr("words[0]")
      .show(2, truncate = false)

    df.select(size(split(col("Description"), " "))).show(2, truncate = false)

    df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2, truncate = false)

    df.withColumn("splitted", split(col("Description"), " "))
      .withColumn("exploded", explode(col("splitted")))
      .select("Description", "splitted", "exploded")
      .show(10, truncate = false)

    // complex types - map
    df.select(map(col("Description"), col("InvoiceNo")).alias("complex")).show(5, truncate = false)

    df.select(map(col("Description"), col("InvoiceNo")).alias("complex"))
      .selectExpr("complex['WHITE METAL LANTERN']")
      .show(5, truncate = false)

    df.select(map(col("Description"), col("InvoiceNo")).alias("complex"))
      .selectExpr("explode(complex)")
      .show(5, truncate = false)

    // json
    val jsonDf = spark
      .range(1)
      .selectExpr("""
                    |'{"myJSONKey" : {"myJSONValue" : [1, 2, 3] }}' as jsonString
                    |""".stripMargin)

    jsonDf
      .select(
        get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]") as "column",
        json_tuple(col("jsonString"), "myJSONKey") as "column2"
      )
      .show(2, truncate = false)

    df.selectExpr("(InvoiceNo, Description) as complex")
      .select(to_json(col("complex")))
      .show(2, truncate = false)

    val parseSchema = new StructType(
      Array(
        StructField("InvoiceNo", StringType, nullable = true),
        StructField("Description", StringType, nullable = true)
      ))
    df.selectExpr("(InvoiceNo, Description) as complex")
      .select(to_json(col("complex")).alias("newJson"))
      .select(from_json(col("newJson"), parseSchema))
      .show(5, truncate = false)

    // user defined functions
    val udfExampleDf                = spark.range(5).toDF("num")
    def power3(num: Double): Double = num * num * num

    val power3DF = udf(power3(_: Double): Double)
    udfExampleDf.select(power3DF(col("num"))).show()

    spark.udf.register("power3", power3(_: Double): Double)
    udfExampleDf.selectExpr("power3(num)").show()

    spark.close()
  }
}
