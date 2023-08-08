package com.damon.rdd.functions

import org.apache.spark.sql.SparkSession

object AggregateExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkByExamples.com")
      .master("local[3]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //aggregate example
    val listRdd = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2))

    val def1 = (accu: Int, v: Int) => accu + v
    val def2 = (accu1: Int, accu2: Int) => accu1 + accu2

    println(listRdd.aggregate(0)(def1, def2))

    val inputRDD = spark.sparkContext.parallelize(List(("Z", 1),("A", 20),("B", 30),("C", 40),("B", 30),("B", 60)))

    val def3 = (accu: Int, v: (String, Int)) => accu + v._2
    val def4 = (accu1: Int, accu2: Int) => accu1 + accu2
    println(inputRDD.aggregate( 0)(def3, def4))


    val def5 = (accu: (String, Int), v: (String, Int)) => (v._1, accu._2 + v._2)
    val def6 = (accu1: (String, Int), accu2: (String, Int)) => (accu1._1, accu1._2 + accu2._2)

    // aggregateByKey 使用的时候需要有一个key，先通过指定的key进行聚合，然后再对key里面的元素再进行聚合
    inputRDD.map(data => (data._1, data))
      .aggregateByKey(("", 0))(def5, def6)
      .foreach(println)

  }
}
