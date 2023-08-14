package com.damon.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DateFormat {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DateFormat")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    Seq(1).toDF("seq")
      .select(
        current_date().as("current_date"),              // returns date in 'yyyy-MM-dd'
        current_timestamp().as("current_time")          // returns date and time in 'yyyy-MM-dd HH:mms:ss.SSS'
      ).show(false)

    Seq(1).toDF("seq")
      .select(
        current_date().as("current_date"),
        date_format(current_timestamp(), "yyyy MM dd").as("yyyy MM dd"),
        date_format(current_timestamp(), "MM/dd/yyyy hh:mm").as("MM/dd/yyyy"),
        date_format(current_timestamp(), "yyyy MMMM dd").as("yyyy MMMM dd"),
        date_format(current_timestamp(), "yyyy MMMM dd E").as("yyyy MMMM dd E")
      ).show(false)

    Seq(("06-03-2009"),("07-24-2009")).toDF("Date")
      .select(
        col("Date"),
        to_date(col("Date"), "MM-dd-yyyy").as("MM-dd-yyyy")
      ).show(false)

    Seq(("2019-07-24"),("07/24/2019"),("2019 Jul 24"),("07-27-19"))
      .toDF("Date")
      .select(
        col("Date"),
        when(
          to_date(col("Date"), "yyyy-MM-dd").isNotNull,
          to_date(col("Date"), "yyyy-MM-dd")
        )
          .when(
            to_date(col("Date"), "MM/dd/yyyy").isNotNull,
            to_date(col("Date"), "MM/dd/yyyy")
          )
          .when(
            to_date(col("Date"), "yyyy MM dd").isNotNull,
            to_date(col("Date"), "yyyy MM dd")
          )
          .otherwise("Unknown Format").as("Formatted Date")
      ).show(false)

    import spark.implicits._
    Seq(("2019-07-24"),("07/24/2019"),("2019 Jul 24"),("07-27-19"))
      .toDF("Date")
      .select(
        col("Date"),
        when(to_date($"Date", "yyyy-MM-dd").isNotNull, date_format(to_date($"Date", "yyyy-MM-dd"), "MM/dd/yyyy"))
          .when(to_date($"Date", "MM/dd/yyyy").isNotNull, date_format(to_date($"Date", "MM/dd/yyyy"), "MM/dd/yyyy"))
//          .when(to_date($"Date", "yyyy MMMM dd").isNotNull, date_format(to_date($"Date", "yyyy MMMM dd"), "MM/dd/yyyy"))
          .otherwise("Unknown Format")
          .as("Formatted Date")
      ).show(false)
  }
}
