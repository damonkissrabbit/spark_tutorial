package com.damon.externalDataSource

import org.apache.spark.sql.SparkSession

object kafkaSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("kafkaSource")
      .master("local[*]")
      .getOrCreate()
  }
}
