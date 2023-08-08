package com.damon.rdd

import org.apache.spark.sql.SparkSession

object ReadMultipleCSVFiles {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ReadMultipleCSVFiles")
      .master("local[*]")
      .getOrCreate()

//    val rdd = spark.sparkContext.textFile("src/main/resources/txt/*")
//    rdd.foreach(println)

    println("------------------------")

    val rdd2 = spark.sparkContext.textFile("src/main/resources/csv/text*.txt")
    rdd2.foreach(println)


  }
}
