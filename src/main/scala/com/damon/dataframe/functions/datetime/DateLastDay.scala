package com.damon.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, last_day, to_date}

object DateLastDay {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DateLastDay")
      .getOrCreate()

    import spark.implicits._

    // last_day 函数用于获取到指定日期月份的最后一天
    Seq(
      ("2019-01-01"),("2020-02-24"),("2019-02-24"),
      ("2019-05-01"),("2018-03-24"),("2007-12-19")
    ).toDF("Date")
      .select(
        col("Date"),
        last_day($"Date").as("last_date")
      ).show(false)

    Seq(("06-03-2009"),("07-24-2009")).toDF("Date")
      .select(
        col("Date"),
        last_day(to_date($"Date", "MM-dd-yyyy")).as("last_day")
      ).show(false)
  }
}
