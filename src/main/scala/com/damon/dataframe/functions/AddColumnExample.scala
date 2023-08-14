package com.damon.dataframe.functions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AddColumnExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkByExamples.com")
      .master("local")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val data = Seq(("111", 50000), ("222", 60000), ("333", 40000))
    val df = data.toDF("EmpId", "Salary")
    df.show(false)


    // derive a new column from existing
    df.withColumn(
      "CopiedColumn", $"salary" * -1
    ).show(false)

    // using select
    df.select(
      $"EmpId", $"Salary", ($"salary" * -1).as("CopiedColumn")
    ).show(false)

    // adding a literal
    val df2 = df.select($"EmpId", $"Salary", lit("1").as("lit_value1"))
    df2.show(false)

    val df3 = df2.withColumn(
      "lit_value2",
      when($"salary" >= 40000 && $"salary" <= 50000, lit("100").cast("int"))
        .otherwise(lit("200").cast("int"))
    )

    df3.printSchema()
    df3.show(false)

    // adding a list column
    val df4 = df3.withColumn(
      "typeLit_seq", typedLit(Seq(1, 2, 3))
    ).withColumn(
      "typeList_map", typedLit(Map("a" -> 1, "b" -> 2))
    ).withColumn(
      "typeList_struct", typedLit(("a", 2, 1.0))
    )

    df4.printSchema()
    df4.show(false)
  }
}
