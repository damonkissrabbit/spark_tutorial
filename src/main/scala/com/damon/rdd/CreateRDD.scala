package com.damon.rdd

import org.apache.spark.sql.SparkSession

object CreateRDD {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("CreateRDD")
      .getOrCreate()

    val rdd = spark.sparkContext.parallelize(Seq(
      ("yuyu", 1000),
      ("damon", 999)
    ))
    rdd.foreach(println)

    // textFile 读取文件的时候，会读取出文件里面的内容
    val rdd1 = spark.sparkContext.textFile("src/main/resources/csv")
    // wholeTextFile 读取文件的时候，会将文件路径作为key，文件里面的内容作为value
    val rdd2 = spark.sparkContext.wholeTextFiles("src/main/resources/csv")

    rdd1.foreach(println)
//    rdd2.foreach(println)
    rdd2.foreach(record => println("FileName: " + record._1 + ", FileContent:" + record._2))

    val rdd3 = rdd.map(row => {
      (row._1, row._2 + 100)
    })
    rdd3.foreach(println)

    val myRdd2 = spark.range(20).toDF().rdd
    myRdd2.foreach(println)
  }
}
