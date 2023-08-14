package com.damon.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UnixTimestamp {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkByExamples.com")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.sqlContext.implicits._

    //Convert Timestamp to Unix timestamp
    val inputDF = Seq(
      ("2019-07-01 12:01:19.000","07-01-2019 12:01:19.000", "07-01-2019")
    ).toDF("timestamp_1","timestamp_2","timestamp_3")

    inputDF.printSchema()
    inputDF.show(false)

    val df = inputDF.select(
      unix_timestamp($"timestamp_1", "yyyy-MM-dd HH:mm:ss.SSS").as("timestamp_1"),
      unix_timestamp($"timestamp_2", "MM-dd-yyyy HH:mm:ss.SSS").as("timestamp_2"),
      unix_timestamp($"timestamp_3", "MM-dd-yyyy").as("timestamp_3"),
      unix_timestamp().as("timestamp_4")
    )

    df.printSchema()
    df.show(false)

    var df2 = df.select(
      from_unixtime($"timestamp_1").as("timestamp_1"),
      from_unixtime($"timestamp_2", "MM-dd-yyyy HH:mm:ss.SSS").as("timestamp_2"),
      from_unixtime($"timestamp_3", "MM-dd-yyyy").as("timestamp_3"),
      from_unixtime($"timestamp_4").as("timesta mp_4")
    )

    df2.printSchema()
    df2.show(false)


    Seq(1).toDF("seq")
      .select(
        from_unixtime(unix_timestamp()).as("timestamp_1"),
        from_unixtime(unix_timestamp(), "MM/dd/yyyy").as("MM/dd/yyyy"),
        from_unixtime(unix_timestamp(), "dd-MM-yyyy HH:mm:ss.SSS").as("dd-MM-yyyy HH:mm:ss.SSS"),
        from_unixtime(unix_timestamp(), "yyyy-MM-dd").as("yyyy-MM-dd")
      ).show(false)

    Seq(1).toDF("seq")
      .select(
        from_unixtime(unix_timestamp(), "MM-dd-yyyy").as("date_1"),
        from_unixtime(unix_timestamp(), "dd-MM-yyyy HH:mm:ss").as("date_2"),
        from_unixtime(unix_timestamp(), "yyyy-MM-dd").as("date_3")
      ).show(false)

  }
}
