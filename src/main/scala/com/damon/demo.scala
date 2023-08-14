package com.damon

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("demo")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      ("damon", 25),
      ("yuyu", 18)
    )

    val schema = Array("name", "age")
    val df = data.toDF(schema:_*)
    df.printSchema()
    df.show(false)

    df.withColumn(
      "lit", lit("love")
    ).show(false)
  }
}
