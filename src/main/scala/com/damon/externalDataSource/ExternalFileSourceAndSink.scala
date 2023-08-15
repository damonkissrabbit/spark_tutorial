package com.damon.externalDataSource

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object ExternalFileSourceAndSink {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ExternalFileSourceAndSink")
      .master("local[*]")
      .getOrCreate()

    spark.read
      .format("csv")
      .option("header", "false")
      .option("mode", "FAILFAST")
      .option("inferSchema", "true")
      .load("src/main/resources/csv_note/dept.csv")
      .show(false)

    val myManualSchema = new StructType(Array(
      StructField("deptno", LongType, nullable = false),
      StructField("dname", StringType, nullable = false),
      StructField("loc", StringType, nullable = false),
    ))

    val df = spark.read
      .format("csv")
      .option("mode", "FAILFAST")
      .schema(myManualSchema)
      .load("src/main/resources/csv_note/dept.csv")

    df.write
      .format("csv")
      .mode("overwrite")
      .save("src/main/TempFiles/ExternalFileSourceAndSink/csv/dept2")

    df.write
      .format("csv")
      .mode("overwrite")
      .option("sep", "\t")
      .save("src/main/TempFiles/ExternalFileSourceAndSink/csv/dept3")

    df.write
      .format("json")
      .mode("overwrite")
      .save("src/main/TempFiles/ExternalFileSourceAndSink/json/dept")


    val parquetDF = spark.read
      .format("parquet")
      .load("src/main/resources/parquet/dept.parquet")
    parquetDF.show(false)

    parquetDF.write
      .format("parquet")
      .mode("overwrite")
      .save("src/main/TempFiles/ExternalFileSourceAndSink/parquet/dept")

    val orcDF = spark.read
      .format("orc")
      .load("src/main/resources/orc/dept.orc")
    orcDF.show(false)

    orcDF.write
      .format("orc")
      .mode("overwrite")
      .save("src/main/TempFiles/ExternalFileSourceAndSink/orc/dept")

  }
}
