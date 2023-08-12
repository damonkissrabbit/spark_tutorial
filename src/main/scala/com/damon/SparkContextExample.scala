package com.damon

import org.apache.spark.sql.{SQLContext, SparkSession}


object SparkContextExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkContextExample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val sqlContext: SQLContext = spark.sqlContext

    val df = sqlContext
      .read
      .options(
        Map(
          "inferSchema" -> "true",
          "delimiter" -> ",",
          "header" -> "true"
        )
      )
      .csv("src/main/resources/zipcodes.csv")

    df.show()
    df.printSchema()

    df.createOrReplaceTempView("TAB")
    sqlContext.sql("select * from TAB").show(false)
//    spark.sql("select * from TAB").show(false)
  }
}
