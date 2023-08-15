package com.damon.dataframe.functions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowGroupByFirst {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val simpleData = Seq(
      ("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("Raman", "Finance", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000)
    )
    val df = simpleData.toDF("employee_name", "department", "salary")
    df.show()

    val windowSpec = Window.partitionBy("department").orderBy("salary")

    df.withColumn(
      "row", row_number().over(windowSpec)
    ).where($"row" === 1).show(false)

    val windowSpec2 = Window.partitionBy("department").orderBy($"salary".desc)
    df.withColumn(
      "row", row_number().over(windowSpec2)
    ).show(false)

    val windowSpec3 = Window.partitionBy("department")
    df.withColumn(
      "row", row_number().over(windowSpec2)
    ).withColumn(
      "max", max("salary").over(windowSpec3)
    ).withColumn(
      "min", min("salary").over(windowSpec3)
    ).withColumn(
      "sum", sum("salary").over(windowSpec3)
    ).withColumn(
      "avg", avg("salary").over(windowSpec3)
    ).show(false)


  }
}
