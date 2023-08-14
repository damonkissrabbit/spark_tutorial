package com.damon.dataframe.functions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, when}

object WhenOtherwise {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val data = List(("James ", "", "Smith", "36636", "M", 60000),
      ("Michael ", "Rose", "", "40288", "M", 70000),
      ("Robert ", "", "Williams", "42114", "", 400000),
      ("Maria ", "Anne", "Jones", "39192", "F", 500000),
      ("Jen", "Mary", "Brown", "", "F", 0))

    val cols = Seq("first_name", "middle_name", "last_name", "dob", "gender", "salary")

    val df = spark.createDataFrame(data).toDF(cols: _*)

    df.withColumn(
      "gender",
      when($"gender" === "M", "Male")
        .when($"gender" === "F", "Female")
        .otherwise("Unknown")
    ).show(false)

    df.withColumn(
      "gender",
      expr(
        "case when gender = 'M' then 'Male' when gender = 'F' then 'Female' else 'Unknown' end"
      )
    ).show(false)

    df.select(
      col("*"),
      when($"gender" === "M", "Male")
        .when($"gender" === "F", "Female")
        .otherwise("Unknown").alias("new_gender")
    ).show(false)

    val dataDF = Seq(
      (66, "a", "4"),
      (67, "a", "0"),
      (70, "b", "4"),
      (71, "d", "4")
    ).toDF("id", "code", "amt")

    dataDF.withColumn(
      "new_column",
      when($"code" === "a" || $"code" === "d", "A")
        .when($"code" === "b" || $"code" === "4", "B")
        .otherwise("A1")
    ).show(false)
  }
}
