package com.damon.rdd.functions

import org.apache.spark.sql.SparkSession

object FoldExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkByExamples.com")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //fold example
    val listRdd = spark.sparkContext.parallelize(List(1, 1, 1, 1, 1, 1, 1, 1))

    println("Partitions: " + listRdd.getNumPartitions)
    println("Total: " + listRdd.fold(2)((a, b) => a + b))

    val inputRDD = spark.sparkContext.parallelize(List(("Z", 1),("A", 20),("B", 30),("C", 40),("B", 30),("B", 60)))
    println("Total: " + inputRDD.fold(("", 0))((a, b) => ("Total", a._2 + b._2)))
  }
}
