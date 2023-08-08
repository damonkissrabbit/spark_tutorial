package com.damon.rdd

import io.netty.handler.codec.smtp.SmtpRequests.data
import org.apache.spark.sql.SparkSession

object RDDPrint {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("RDDPrint")
      .master("local[*]")
      .getOrCreate()

    val dept = List(
      ("Finance", 10), ("Marketing", 20),
      ("Sales", 30), ("IT", 40)
    )

    val rdd = spark.sparkContext.parallelize(dept)

    val dataColl = rdd.collect()
    println(dataColl)
    dataColl.foreach(println)

    val dataCollLis = rdd.collectAsMap()
    dataCollLis.foreach(data => println(data._1 + "," + data._2))

  }
}
