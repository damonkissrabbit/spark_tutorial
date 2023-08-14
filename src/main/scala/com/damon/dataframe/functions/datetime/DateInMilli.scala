package com.damon.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

object DateInMilli {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DateInMilli")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = Seq(1).toDF("seq")
      .select(
        current_date().as("current_date"),
        unix_timestamp().as("unix_timestamp")
      )
    df.printSchema()
    df.show(false)

    // convert unix seconds to date
    df.select(
      to_date($"unix_timestamp".cast(TimestampType)).as("current_date")
    ).show(false)

    df.select(
      from_unixtime($"unix_timestamp", "yyyy-MM-dd HH:mm:ss").as("from_unixtime")
    ).show(false)

    df.select(
      unix_timestamp($"current_date").as("unix_seconds"),
      unix_timestamp(lit("12-21-2019"), "MM-dd-yyyy").as("unix_seconds2")
    ).show(false)
  }
}
