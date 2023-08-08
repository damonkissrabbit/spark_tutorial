package com.damon.rdd.functions

import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object FlatMapFunction {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("FlatMapFunction")
      .master("local[*]")
      .getOrCreate()

    val data = Seq(
      "Project Gutenberg’s",
      "Alice’s Adventures in Wonderland",
      "Project Gutenberg’s",
      "Adventures in Wonderland",
      "Project Gutenberg’s"
    )

    val rdd = spark.sparkContext.parallelize(data)
//
//    rdd.flatMap(data => data.split(" "))
//      .foreach(println)


    val arrayStructureData = Seq(
      Row("James,,Smith",List("Java","Scala","C++"),"CA"),
      Row("Michael,Rose,",List("Spark","Java","C++"),"NJ"),
      Row("Robert,,Williams",List("CSharp","VB","R"),"NV")
    )

    val arrayStruct = new StructType()
      .add("name", StringType)
      .add("languageAtSchool", ArrayType(StringType))
      .add("currentState", StringType)

    import spark.implicits._
    spark.createDataFrame(spark.sparkContext.parallelize(arrayStructureData), arrayStruct)
      .flatMap(data => {
        data.getSeq[String](1).map((data.getString(0), _, data.getString(2))).foreach(println)
        println("----------------------------------")
        data.getSeq[String](1).map((data.getString(0), _, data.getString(2)))
      })
      .toDF("name", "language", "state").show(false)
  }
}
