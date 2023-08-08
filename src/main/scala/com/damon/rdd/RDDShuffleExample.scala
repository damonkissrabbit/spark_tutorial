package com.damon.rdd

import io.netty.handler.codec.smtp.SmtpRequests.data
import org.apache.spark.sql.SparkSession

object RDDShuffleExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("RDDShuffleExample")
      .master("local[*]")
      .getOrCreate()

    val rdd = spark.sparkContext.textFile("src/main/resources/test.txt")
    println(rdd.getNumPartitions)

    val rdd2 = rdd.flatMap(data => data.split(" "))
      .map(data => (data, 1))
      .reduceByKey(_+_)
    rdd2.foreach(println)

    println(rdd2.getNumPartitions)
  }
}

