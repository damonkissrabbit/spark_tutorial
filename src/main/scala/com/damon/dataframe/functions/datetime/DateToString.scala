package com.damon.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DateToString {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DateToString")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // MMMM 使用的是英文的月的全写 August， MMM 使用的是因为的月的缩写 Aug
    Seq(1).toDF("seq")
      .select(
        date_format(current_timestamp(), "yyyy MM dd"),
        date_format(current_timestamp(), "MM/dd/yyyy hh:mm").as("MM/dd/yyyy"),
        date_format(current_timestamp(), "yyyy MMM dd").as("yyyy MMM dd"),
        date_format(current_timestamp(), "yyyy MMMM dd E").as("yyyy MMMM dd E")
      ).show(false)
  }
}
