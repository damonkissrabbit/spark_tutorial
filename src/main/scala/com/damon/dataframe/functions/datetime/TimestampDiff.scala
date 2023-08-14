package com.damon.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TimestampDiff {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkByExamples.com")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.sqlContext.implicits._

    //Difference between two timestamps
    val df = Seq(
      ("2019-07-01 12:01:19.000"),
      ("2019-06-24 12:01:19.000"),
      ("2019-11-16 16:44:55.406"),
      ("2019-11-16 16:50:59.406")
    ).toDF("input_timestamp")

    df.withColumn(
      "input_timestamp", to_timestamp(col("input_timestamp"))
    ).withColumn(
      "current_timestamp", current_timestamp()
    ).withColumn(
      "diffInSeconds", current_timestamp().cast(LongType) - col("input_timestamp").cast("long")
    ).withColumn(
      "diffInMinutes", round(col("diffInSeconds") / 60)
    ).withColumn(
      "diffInHours", round(col("diffInSeconds") / 3600)
    ).withColumn(
      "diffInDays", round(col("diffInSeconds") / 24 * 3600)
    ).show(false)


    val df1 = Seq(
      ("12:01:19.000","13:01:19.000"),
      ("12:01:19.000","12:02:19.000"),
      ("16:44:55.406","17:44:55.406"),
      ("16:50:59.406","16:44:59.406")
    )
      .toDF("from_timestamp","to_timestamp")

    df1.withColumn(
      "from_timestamp", to_timestamp(col("from_timestamp"), "HH:mm:ss.SSS")
    ).withColumn(
      "to_timestamp", to_timestamp(col("to_timestamp"), "HH:mm:ss.SSS")
    ).withColumn(
      "diffInSeconds", col("to_timestamp").cast("long") - col("from_timestamp").cast("long")
    ).show(false)


    val dfDate = Seq(
      ("07-01-2019 12:01:19.406"),
      ("06-24-2019 12:01:19.406"),
      ("11-16-2019 16:44:55.406"),
      ("11-16-2019 16:50:59.406")
    ).toDF("input_timestamp")

    dfDate.withColumn(
      "input_timestamp", to_timestamp(col("input_timestamp"), "MM-dd-yyyy HH:mm:ss.SSS")
    ).withColumn(
      "diffInSeconds", current_timestamp().cast("long") - col("input_timestamp").cast("long")
    ).show(false)



  }
}
