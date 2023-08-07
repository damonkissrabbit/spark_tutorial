package com.damon.rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object RDDFromDataUsingParallelize {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .appName("RDDFromDataUsingParallelize")
        .master("local[*]")
        .getOrCreate()

    val rdd: RDD[Int] = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5))
    val rddCollect: Array[Int] = rdd.collect()

    println("Number of partitions: " + rdd.getNumPartitions)
    println("Action: First element: " + rdd.first())
    println("Action: RDD converted to Array[Int]: ")
    rddCollect.foreach(println)

    val rdd2 = spark.sparkContext.parallelize(List(1 to 1000))
    println("Number of partitions: " + rdd2.getNumPartitions)

    val rdd3 = rdd2.repartition(10)
    println("Number of paritions: "+ rdd3.getNumPartitions)
  }
}
