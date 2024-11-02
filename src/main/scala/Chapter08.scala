import org.apache.spark.sql.functions._

object Chapter08 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionProvider.spark
    
    import spark.implicits._
    
    val person = Seq(
      (0, "Bill Chambers", 0, Seq(100)),
      (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
      (2, "Michael Armbrust", 1, Seq(250, 100)))
      .toDF("id", "name", "graduate_program", "spark_status")
    val graduateProgram = Seq(
      (0, "Masters", "School of Information", "UC Berkeley"),
      (2, "Masters", "EECS", "UC Berkeley"),
      (1, "Ph.D.", "EECS", "UC Berkeley"))
      .toDF("id", "degree", "department", "school")
    val sparkStatus = Seq(
      (500, "Vice President"),
      (250, "PMC Member"),
      (100, "Contributor"))
      .toDF("id", "status")
    
    person.createOrReplaceTempView("person")
    graduateProgram.createOrReplaceTempView("graduateProgram")
    sparkStatus.createOrReplaceTempView("sparkStatus")
    
    spark.sql("SELECT * FROM person").show(truncate = false)
    spark.sql("SELECT * FROM graduateProgram").show(truncate = false)
    spark.sql("SELECT * FROM sparkStatus").show(truncate = false)
    
    // Inner Join
    val JoinExpr = person.col("graduate_program") === graduateProgram.col("id")
    var joinType = "inner"
    person.join(graduateProgram, JoinExpr).show(truncate = false)
    person.join(graduateProgram, JoinExpr, joinType).show(truncate = false)
    
    val innerJoinQuery: String =
      s"""
         |select * from person as p
         |join graduateProgram as g on p.graduate_program = g.id
         |""".stripMargin
    spark.sql(innerJoinQuery).show(truncate = false)
    
    // Outer Join
    joinType = "outer"
    person.join(graduateProgram, JoinExpr, joinType).show(truncate = false)
    
    val outerJoinQuery: String =
      s"""
         |select * from person as p
         |full outer join graduateProgram as g on p.graduate_program = g.id
         |""".stripMargin
    spark.sql(outerJoinQuery).show(truncate = false)
    
    // Left Outer Join
    joinType = "left_outer"
    person.join(graduateProgram, JoinExpr, joinType).show(truncate = false)
    
    val leftOuterJoinQuery: String =
      s"""
         |select * from person as p
         |left outer join graduateProgram as g on p.graduate_program = g.id
         |""".stripMargin
    spark.sql(leftOuterJoinQuery).show(truncate = false)
    
    // Right Outer Join
    joinType = "right_outer"
    person.join(graduateProgram, JoinExpr, joinType).show(truncate = false)
    
    val rightOuterJoinQuery: String =
      s"""
         |select * from person as p
         |right outer join graduateProgram as g on p.graduate_program = g.id
         |""".stripMargin
    spark.sql(rightOuterJoinQuery).show(truncate = false)
    
    // Left Semi Join
    joinType = "left_semi"
    graduateProgram.join(person, JoinExpr, joinType).show(truncate = false)
    
    val leftSemiJoinQuery: String =
      s"""
         |SELECT * FROM graduateProgram
         |LEFT SEMI JOIN person ON graduateProgram.id = person.graduate_program
         |""".stripMargin
    spark.sql(leftSemiJoinQuery).show(truncate = false)
    
    val gradProgram2 = graduateProgram.union(Seq(
      (0, "Masters", "Duplicated Row", "Duplicated School")).toDF())
    gradProgram2.createOrReplaceTempView("gradProgram2")
    spark.sql("select * from gradProgram2").show(truncate = false)
    gradProgram2.join(person, JoinExpr, joinType).show(truncate = false)
    
    // Left Anti Join
    joinType = "left_anti"
    graduateProgram.join(person, JoinExpr, joinType).show(truncate = false)
    
    val leftAntiJoinQuery: String =
      s"""
         |SELECT * FROM graduateProgram
         |LEFT ANTI JOIN person ON graduateProgram.id = person.graduate_program
         |""".stripMargin
    spark.sql(leftAntiJoinQuery).show(truncate = false)
    
    // Cross Join
    joinType = "cross"
    graduateProgram.crossJoin(person).show(numRows = 20, truncate = false)
    graduateProgram.join(person, JoinExpr, joinType).show(numRows = 20, truncate = false)
    
    val crossJoinQuery: String =
      s"""
         |SELECT * FROM graduateProgram
         |CROSS JOIN person
         |""".stripMargin
    spark.sql(crossJoinQuery).show(truncate = false)
    
    // Join with complex types
    joinType = "inner"
    person
      .withColumnRenamed("id", "personId")
      .join(sparkStatus, expr("array_contains(spark_status, id)"))
      .show(truncate = false)
    
    // Duplicated columns names
    val gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")
    gradProgramDupe.show(truncate = false)
    val joinExpr = gradProgramDupe.col("graduate_program") === person.col("graduate_program")
    gradProgramDupe.show(truncate = false)
    person.join(gradProgramDupe, joinExpr).show()
    
    // this will throw an error
    //    person.join(gradProgramDupe, joinExpr).select("graduate_program").show(truncate = false)
    
    // Approach 1: different join expression
    person.join(gradProgramDupe, "graduate_program").show()
    person.join(gradProgramDupe, "graduate_program").select("graduate_program").show()
    // Approach 2:  dropping the column after join
    person.join(gradProgramDupe, joinExpr).drop(person.col("graduate_program")).show()
    person.join(gradProgramDupe, joinExpr).drop(person.col("graduate_program")).select("graduate_program").show()
    // Approach 3: Renaming a column before the join
    val gradProgram3 = graduateProgram.withColumnRenamed("id", "grad_id")
    val joinExpr2 = person.col("graduate_program") === gradProgram3.col("grad_id")
    person.join(gradProgram3, joinExpr2).show()
    
    val joinExpr3 = person.col("graduate_program") === graduateProgram.col("id")
    person.join(graduateProgram, joinExpr3).explain()
    person.join(broadcast(graduateProgram), joinExpr3).explain()
    
    val broadcastQuery: String =
      s"""
         |select /*+ MAPJOIN(graduateProgram) */ * from person
         |join graduateProgram
         |on person.graduate_program = graduateProgram.id
         |""".stripMargin
    spark.sql(broadcastQuery).show(truncate = false)
    
    spark.stop()
  }
}
