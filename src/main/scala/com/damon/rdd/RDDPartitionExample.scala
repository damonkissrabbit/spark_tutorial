package com.damon.rdd

import org.apache.spark.sql.SparkSession

object RDDPartitionExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("RDDPartitionExample")
      .master("local[*]")
      .getOrCreate()

    val rdd = spark.sparkContext.parallelize(Range(0, 20))
    println("From local[*] " + rdd.partitions.length)

    val rddFromFile = spark.sparkContext.textFile("src/main/resources/test.txt", 9)
    println("TextFile: " + rddFromFile.partitions.length)

    val rdd3 = rddFromFile.coalesce(4)
    println("Coalesce: " + rdd3.getNumPartitions)
  }
}
