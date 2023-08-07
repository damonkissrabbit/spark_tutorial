package com.damon.rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD


case class zipcode(recordNumber: Int, ZipCode: String, city: String, state: String)

object RDDCache {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
        .builder()
        .appName("RDDCache")
        .master("local[*]")
        .getOrCreate()
    
    val sc = spark.sparkContext

    val rdd = sc.textFile("src/main/resources/zipcodes-noheader.csv")

    val rdd2: RDD[zipcode] = rdd.map(data => {
        val dataArrays = data.split(",")
        zipcode(dataArrays(0).toInt, dataArrays(1), dataArrays(3), dataArrays(4))
    })

    rdd2.cache()
    
    println(rdd2.count())

    println("--------------")

    rdd2.foreach(data => println(data.city))
  }
}
