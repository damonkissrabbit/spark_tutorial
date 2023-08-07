package com.damon.rdd

import org.apache.spark.sql.SparkSession

object RDDAccumulator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .appName("RDDAccumulator")
        .master("local[*]")
        .getOrCreate()
    
    val longAcc = spark.sparkContext.longAccumulator("SumAccumulator")

    val rdd = spark.sparkContext.parallelize(Array(1, 2, 3))

    rdd.foreach(x => longAcc.add(x))
    println(longAcc.value)
  }
}
