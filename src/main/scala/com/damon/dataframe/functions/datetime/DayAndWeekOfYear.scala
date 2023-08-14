package com.damon.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DayAndWeekOfYear {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("DayAndWeekOfYear")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.sqlContext.implicits._

    val df = Seq(
      ("2019-01-03 12:01:19.000"),
      ("2019-02-01 12:01:19.000"),
      ("2019-7-16 16:44:55.406"),
      ("2019-11-16 16:50:59.406")
    ).toDF("input_timestamp")

    // Get day of the year
    df.withColumn(
      "input_timestamp", to_timestamp(col("input_timestamp"))
    ).withColumn(
      "day_of_year", date_format(col("input_timestamp"), "D")
    ).show(false)

    // Get week of the year     (version error)
    df.withColumn(
      "input_timestamp", to_timestamp(col("input_timestamp"))
    ).withColumn(
      "week_of_year", date_format(col("input_timestamp"), "W")
    ).show(false)
  }
}
