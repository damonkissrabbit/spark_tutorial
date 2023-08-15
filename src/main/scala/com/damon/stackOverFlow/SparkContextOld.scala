package com.damon.stackOverFlow

import org.apache.spark.{SparkConf, SparkContext}

object SparkContextOld {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SparkContextOld")
      .setMaster("local[*]")

    val sparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("ERROR")

    val rdd = sparkContext.textFile("src/main/resources/txt/alice.txt")
    println("App Name: " + sparkContext.appName)
    println("Deploy Mode: " + sparkContext.deployMode)
    println("Master: " + sparkContext.master)
    println("ApplicationId: " + sparkContext.applicationId)
  }
}
