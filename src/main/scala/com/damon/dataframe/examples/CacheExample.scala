package com.damon.dataframe.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object CacheExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("CacheExample")
      .master("local[*]")
      .getOrCreate()

    val df = spark
      .read
      .options(Map(
        "inferSchema" -> "true",
        "delimiter" -> ",",
        "header" -> "true"
      ))
      .csv("src/main/resources/zipcodes.csv")

    df.show(false)

    val df2 = df.where(col("State") === "PR").cache()
    df2.show(false)
    println(df2.count())

    val df3 = df2.where(col("Zipcode") === 704)
    df3.show(false)
    println(df3.count())

    df2.unpersist()
  }
}
