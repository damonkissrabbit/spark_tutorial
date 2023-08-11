package com.damon.dataframe.functions.datetime

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DateDiff {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("DateDiff")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.sqlContext.implicits._

    val df1 = Seq(("2019-07-01"), ("2019-06-24"), ("2019-08-24"), ("2018-07-23")).toDF("date")
    df1.show()
    df1.select(
      $"date",
      current_date().as("current_date"),
      datediff(current_date(), $"date").as("dateDiff")
    ).show(false)

    val df2 = Seq(("2019-07-01"), ("2019-06-24"), ("2019-08-24"), ("2018-12-23"), ("2018-07-20"))
      .toDF("startDate")
      .select(
        $"startDate",
        current_date().as("endDate")
      )

    calculateDiff(df2)

    val dfDate = Seq(("07-01-2019"), ("06-24-2019"), ("08-24-2019"), ("12-23-2018"), ("07-20-2018"))
      .toDF("startDate")
      .select(
        to_date(col("startDate"), "MM-dd-yyyy").as("startDate"),
        current_date().as("endDate")
      )

    calculateDiff(dfDate)
  }

  def calculateDiff(df: DataFrame): Unit = {
    df.withColumn(
      "datesDiff", datediff(col("endDate"), col("startDate"))
    ).withColumn(
      "monthsDiff", months_between(col("endDate"), col("startDate"))
    ).withColumn(
      "monthsDifRound", round(months_between(col("endDate"), col("startDate")), 2)
    ).withColumn(
      "yearsDiff", months_between(col("endDate"), col("startDate"), roundOff = true).divide(12)
    ).withColumn(
      "yearDiffRound", round(months_between(col("endDate"), col("startDate"), roundOff = true).divide(12), 2)
    ).show(false)
  }
}
