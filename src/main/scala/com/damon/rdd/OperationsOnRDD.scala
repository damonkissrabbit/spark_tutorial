package com.damon.rdd

import org.apache.spark.sql.SparkSession

object OperationsOnRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("OperationsOnRDD")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val rdd = spark.sparkContext.parallelize(
      List("Germany India USA","USA London Russia","Mexico Brazil Canada China")
    )

    val listRDD = spark.sparkContext.parallelize(List(9,2,3,4,5,6,7,8))

    // reduce
    println("Minimum: " + listRDD.reduce((a, b) => a min b))
    println("Maximum: " + listRDD.reduce((a, b) => a max b))
    println("Sum: " + listRDD.reduce((a, b) => a + b))

    // flatMap
    val wordsRdd = rdd.flatMap(_.split(" "))
    wordsRdd.foreach(println)

    // sortBy
    println("sort by word name")
    wordsRdd.sortBy(data => data).foreach(println)

    // groupBy
    val groupRdd = wordsRdd.groupBy(word => word.length)
    groupRdd.foreach(println)

    // map
    val tuple2Rdd = wordsRdd.map(data => (data, 1))
    tuple2Rdd.foreach(println)
  }
}
