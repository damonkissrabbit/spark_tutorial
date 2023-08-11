package com.damon.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

object AddTime {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("AddTime")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // TIMESTAMP 是一个类型
    spark.sql(
      "select current_timestamp, cast(current_timestamp as TIMESTAMP) + INTERVAL 2 hours as added_hours, " +
        "cast(current_timestamp as TIMESTAMP) + INTERVAL 5 minutes as added_minutes, " +
        "cast(current_timestamp as TIMESTAMP) + INTERVAL 5 seconds as added_seconds"
    ).show(false)

    val df = Seq(
      ("2019-07-01 12:01:19.101"),
      ("2019-06-24 12:01:19.222"),
      ("2019-11-16 16:44:55.406"),
      ("2019-11-16 16:50:59.406")
    ).toDF("input_timestamp")

    df.createOrReplaceTempView("addTimeExample")

    spark.sql(
      "select input_timestamp, cast(input_timestamp as TIMESTAMP) + INTERVAL 2 hours as added_hours, " +
      "cast(input_timestamp as TIMESTAMP) + INTERVAL 5 minutes as added_minutes, " +
      "cast(input_timestamp as TIMESTAMP) + INTERVAL 5 seconds as added_seconds from addTimeExample"
    ).show(false)

    df.withColumn(
      "added_hours", col("input_timestamp") + expr("INTERVAL 2 HOURS")
    ).withColumn(
      "added_minutes", col("input_timestamp") + expr("INTERVAL 2 minutes")
    ).withColumn(
      "added_seconds", col("input_timestamp") + expr("INTERVAL 2 seconds")
    ).show(false)
  }
}
