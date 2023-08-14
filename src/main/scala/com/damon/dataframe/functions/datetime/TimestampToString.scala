package com.damon.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TimestampToString {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkByExamples.com")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.sqlContext.implicits._

    Seq(1).toDF("seq")
      .select(
        current_timestamp().as("current_date"),
        date_format(current_timestamp(), "yyyy MM dd").as("yyyy MM dd"),
        date_format(to_date(current_timestamp(), "yyyy-MM-dd"), "yyyy MMM")
      ).show(false)
  }
}
