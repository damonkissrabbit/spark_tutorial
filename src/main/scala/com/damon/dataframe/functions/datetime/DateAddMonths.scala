package com.damon.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DateAddMonths {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkByExamples.com")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.sqlContext.implicits._
    val df1 = Seq(("2019-01-23"), ("2019-06-24"), ("2019-09-20")).toDF("date")
    df1.show(false)

    df1.select(
      $"date",
      add_months($"date", 3).as("add_months"),
      add_months($"date", -3).as("sub_months"),
      date_add($"date", 3).as("date_add"),
      date_sub($"date", 3).as("date_sub")
    ).show(false)

    val df2 = Seq(("06-03-2009"), ("07-24-2009")).toDF("date")
    df2.show(false)

    df2.select(
      $"date",
      add_months(to_date($"date", "MM-dd-yyyy"), 3).as("add_months"),
      add_months(to_date($"date", "MM-dd-yyyy"), -3).as("sub_months"),
      date_add(to_date($"date", "MM-dd-yyyy"), 3).as("date_add"),
      date_add(to_date($"date", "MM-dd-yyyy"), -3).as("date_sub"),
      date_sub(to_date($"date", "MM-dd-yyyy"), -3).as("date_sub")
    ).show(false)

    val df3 = Seq(("2019-01-23", 1), ("2019-06-24", 2), ("2019-09-20", 3))
      .toDF("date", "increment")

    df3.select(
      $"date",
      $"increment",
      expr("add_months(to_date(date, 'yyyy-MM-dd'), cast(increment as int))").as("inc_date")
    ).show(false)

    val df4 = Seq(("2019-01-23", 1), ("2019-06-24", 2), ("2019-09-20", 3))
      .toDF("date", "increment")

    df4.selectExpr(
      "date",
      "increment",
      "add_months(to_date(date, 'yyyy-MM-dd'), cast(increment as int)) as inc_date"
    ).show(false)

  }
}
