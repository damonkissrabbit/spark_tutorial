package com.damon.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DateTrunc {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("DateTrunc")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    Seq("1").toDF("Date")
      .select(
        current_timestamp(),
        trunc(current_timestamp(), "Year").as("Year"),
        trunc(current_timestamp(), "Month").as("Month"),
        trunc(current_timestamp(), "Day").as("Day")
      ).show(false)


    Seq("1").toDF("Date")
      .select(
        current_timestamp(),
        date_trunc("Year", current_timestamp()).as("Year"),
        date_trunc("Month", current_timestamp()).as("Month"),
        date_trunc("Day", current_timestamp()).as("Day"),
        date_trunc("Hour", current_timestamp()).as("Hour"),
        date_trunc("Minute", current_timestamp()).as("Minute")
      ).show(false)

    Seq(("2019-01-23"),("2019-06-24"),("2019-09-20")).toDF("date")
      .select(
        col("date"),
        trunc(col("date"), "Month").as("Month"),
        trunc(col("date"), "Year").as("Year"),
      ).show(false)

    Seq(("01-23-2019"),("06-24-2019"),("09-20-2019")).toDF("date")
      .select(
        col("date"),
        trunc(to_date(col("date"), "MM-dd-yyyy"), "Month").as("Month"),
        trunc(to_date(col("date"), "MM-dd-yyyy"), "Year").as("Year")
      ).show(false)

    Seq("1").toDF("date")
      .select(
        current_timestamp(),
        trunc(current_timestamp(), "Year").as("Year_Begin"),
        trunc(current_timestamp(), "Month").as("Month_Begin")
      ).show(false)

    Seq("1").toDF("date")
      .select(
        current_timestamp(),
        last_day(current_timestamp()).as("Month_End")
      ).show(false)

    Seq("1").toDF("date")
      .select(
        current_timestamp(),
        trunc(add_months(current_timestamp(), 1), "Month").as("Next_Month_Begin")
      ).show(false)

    Seq("1").toDF("date")
      .select(
        current_timestamp(),
        trunc(add_months(current_timestamp(), -1), "Month").as("Previous_Month_Begin")
      ).show(false)


  }
}
