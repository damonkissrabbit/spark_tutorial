package com.damon.dataset

import org.apache.spark.sql.SparkSession

object DataSetWithCustomClass {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("DataSetWithCustomClass")
      .getOrCreate()

    val data = Seq((1, 2), (3, 4), (5, 6))

    val set = data.toSet
    set.foreach(println)
  }
}
