package com.damon.rdd

import org.apache.spark.sql.SparkSession

object CreateEmptyRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("CreateEmptyRDD")
      .getOrCreate()

    val rdd = spark.sparkContext.emptyRDD
    val rddString = spark.sparkContext.emptyRDD[String]

    println(rdd)
    println(rddString)
    println("Num of Partitions: " + rdd.getNumPartitions)

    rddString
      .saveAsTextFile("TempFiles/rdd/text5.txt")

    val rdd2 = spark.sparkContext.parallelize(Seq.empty[String])
    println(rdd2)
    println("Num of Partitions: " + rdd2.getNumPartitions)

    type dateType = (String, Int)
    val pairRDD = spark.sparkContext.emptyRDD[dateType]
    println(pairRDD)
  }
}
