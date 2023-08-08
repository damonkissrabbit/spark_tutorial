package com.damon.rdd

import io.netty.handler.codec.smtp.SmtpRequests.data
import org.apache.spark.sql.SparkSession

object SoryBy {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SortBy")
      .master("local[*]")
      .getOrCreate()

    val rdd = spark.sparkContext.textFile("src/main/resources/zipcodes-noheader.csv")
    val rddZipcode = rdd.map(data => {
      val dataArrays = data.split(",")
      zipcode(dataArrays(0).toInt, dataArrays(1), dataArrays(3), dataArrays(4))
    })

    rddZipcode
      .sortBy(data => data.recordNumber)
      .foreach(println)

    println("-----------------")
    rddZipcode.map(data => Tuple2(data.recordNumber, data.toString))
      .sortByKey(ascending = false).foreach(data => data._2)
  }
}
