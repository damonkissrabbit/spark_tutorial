package com.damon.rdd

import io.netty.handler.codec.smtp.SmtpRequests.data
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

import java.io.File

object PartitionBy {
  def main(args: Array[String]): Unit = {

    val outputPath = "src/main/TempFiles/rdd/output"


    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("PartitionBy")
      .getOrCreate()

    val sc = spark.sparkContext

    val folder = new File(outputPath)
    if (folder.exists() && folder.isDirectory) {
      deleteFolder(folder)
    }

    sc.textFile("src/main/resources/zipcodes.csv")
      .map(data => {
        val dataArrays: Array[String] = data.split(",")
        (dataArrays(1), dataArrays.mkString(","))
      })
      .partitionBy(new HashPartitioner(3))
      .saveAsTextFile(outputPath)
  }

  def deleteFolder(file: File): Unit = {
    if (file.isDirectory){
      val children = file.listFiles()
      if (children != null) {
        for (child <- children) {
          deleteFolder(child)
        }
      }
    }
    file.delete()
  }
}
