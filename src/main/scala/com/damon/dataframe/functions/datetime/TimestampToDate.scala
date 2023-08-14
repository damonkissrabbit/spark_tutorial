package com.damon.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

object TimestampToDate {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkByExamples.com")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.sqlContext.implicits._

    val df = Seq(
      ("2019-07-01 12:01:19.000"),
      ("2019-06-24 12:01:19.000"),
      ("2019-11-16 16:44:55.406"),
      ("2019-11-16 16:50:59.406")
    ).toDF("input_timestamp")

    val result = df.withColumn(
      "ts", to_timestamp($"input_timestamp", "yyyy-MM-dd HH:mm:ss.SSS")
    ).withColumn(
      "dateType_1", to_date($"ts")
    ).withColumn(
      "dateType_2", $"ts".cast(DateType)
    ).withColumn(
      "unix_timestamp", unix_timestamp()
    ).withColumn(
      "from_unix", from_unixtime($"unix_timestamp")
    ).withColumn(
      "cast_unix", to_timestamp($"from_unix", "yyyy-MM-dd HH:mm:ss")
    ).withColumn(
      "date", to_date($"cast_unix", "yyyy MM")
    )

    result.printSchema()
    result.show(false)
  }
}
