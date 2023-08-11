package com.damon.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CurrentDateAndTime {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("CurrentDateAndTime")
      .master("local[*]")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val df = Seq((1)).toDF("Seq")

    val curDate = df.withColumn(
      "current_date", current_date().as("current_date")
    ).withColumn(
      "current_timestamp", current_timestamp().as("current_timestamp")
    )
    curDate.show(false)

    curDate.select(
      date_format($"current_timestamp", "MM-dd-yyyy").as("date"),
      date_format($"current_timestamp", "HH:mm:ss.SSS").as("time"),
      date_format($"current_timestamp", "MM-dd-yyyy").as("current_date_formated")
    ).show(false)
  }
}
