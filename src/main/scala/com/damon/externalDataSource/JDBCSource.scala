package com.damon.externalDataSource

import org.apache.spark.sql.SparkSession

object JDBCSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("JDBCSource")
      .master("local[*]")
      .getOrCreate()

//    spu_info
    spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://192.168.0.114:3306/gmail_flink")
      .option("dbtable", "spu_info")
      .option("user", "root")
      .option("password", "123")
      .load()
      .show(false)
  }
}
