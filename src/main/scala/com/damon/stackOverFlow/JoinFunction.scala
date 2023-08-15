package com.damon.stackOverFlow

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object JoinFunction {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("JoinFunction")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val df1: Dataset[Row] = Seq(
      ("Mark", "2018-02-20 00:00:00"),
      ("Alex", "2018-03-01 00:00:00"),
      ("Bob", "2018-03-01 00:00:00"),
      ("Mark", "2018-07-01 00:00:00"),
      ("Kate", "2018-07-01 00:00:00")
    ).toDF("USER_NAME", "REQUEST_DATE")
    df1.show()

    val df2: Dataset[Row] = Seq(
      ("Alex", "2018-01-01 00:00:00", "2018-02-01 00:00:00", "OUT"),
      ("Bob", "2018-02-01 00:00:00", "2018-02-05 00:00:00", "IN"),
      ("Mark", "2018-02-01 00:00:00", "2018-03-01 00:00:00", "IN"),
      ("Mark", "2018-05-01 00:00:00", "2018-08-01 00:00:00", "OUT"),
      ("Meggy", "2018-02-01 00:00:00", "2018-02-01 00:00:00", "OUT")
    ).toDF("NAME", "START_DATE", "END_DATE", "STATUS")
    df2.show()

    val df3 = df1.join(df2, $"USER_NAME" === $"NAME", "left_outer")

    val df4: Dataset[Row] = df3.withColumn(
      "USER_STATUS",
      when($"REQUEST_DATE" > $"START_DATE" and $"REQUEST_DATE" < $"END_DATE", "Our user")
        .otherwise("Not our user.")
    )

    df4.show(false)

    df4.select("USER_NAME","REQUEST_DATE","USER_STATUS").show(false)
    df4.select("USER_NAME","REQUEST_DATE","USER_STATUS").dropDuplicates("USER_NAME").show(false)

  }
}
