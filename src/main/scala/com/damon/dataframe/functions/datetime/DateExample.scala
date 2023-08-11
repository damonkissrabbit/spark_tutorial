package com.damon.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DateExample {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val data = Seq(("2019-01-23"), ("2019-06-24"), ("2019-09-20"))

    import spark.sqlContext.implicits._
    val df = data.toDF("date")

    df.show(false)

    Seq(("2019-01-23")).toDF("InputDate").select(
      current_date() as ("current_date"),
      col("InputDate"),
      date_format(col("InputDate"), "MM-dd-yyyy").as("date_format")
    ).show(false)

    Seq(("04/13/2019")).toDF("InputDate").select(
      col("InputDate"),
      to_date(col("InputDate"), "MM/dd/yyyy").as("to_date")
    ).show()

    Seq(("2019-01-23"), ("2019-06-24"), ("2019-09-20")).toDF("date")
      .select(
        col("date"),
        current_date(),
        datediff(current_date(), col("date")).as("dateDiff")
      ).show()

    Seq(("2019-01-23"), ("2019-06-24"), ("2019-09-20")).toDF("date")
      .select(
        col("date"),
        current_date(),
        datediff(current_date(), col("date")).as("dateDiff"),
        months_between(current_date(), col("date")).as("months_between")
      ).show(false)

    Seq(("2019-01-23"), ("2019-06-24"), ("2019-09-20")).toDF("date")
      .select(
        col("date"),
        trunc(col("date"), "Month").as("Month_Trunc"),
        trunc(col("date"), "Year").as("Month_Year"),
      ).show(false)

    Seq(("2019-01-23"), ("2019-06-24"), ("2019-09-20")).toDF("date").select(
      col("date"),
      add_months(col("date"), 3).as("add_months"),
      add_months(col("date"), -3).as("sub_months"),
      date_add(col("date"), 4).as("date_add"),
      date_sub(col("date"), 4).as("date_sub")
    ).show()

    // next_day 算子是算出下一个周几的日期
    Seq(("2019-01-23"),("2019-06-24"),("2019-09-20")).toDF("date")
      .select(
        col("date"),
        year(col("date")).as("year"),
        month(col("date")).as("month"),
        dayofweek(col("date")).as("dayOfWeek"),
        dayofmonth(col("date")).as("dayOfMonth"),
        dayofyear(col("date")).as("dayOfYear"),
        next_day(col("date"), "Sunday").as("next_day"),
        weekofyear(col("date")).as("weekOfYear")
      ).show(false)

  }
}
