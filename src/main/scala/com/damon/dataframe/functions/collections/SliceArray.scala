package com.damon.dataframe.functions.collections

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object SliceArray {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkByExamples.com")
      .master("local")
      .getOrCreate()

    val arrayStructureData = Seq(
      Row("James,,Smith",List("Java","Scala","C++","Pascal","Spark")),
      Row("Michael,Rose,",List("Spark","Java","C++","Scala","PHP")),
      Row("Robert,,Williams",List("CSharp","VB",".Net","C#.net",""))
    )

    val arrayStructureSchema = new StructType()
      .add("name",StringType)
      .add("languagesAtSchool", ArrayType(StringType))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
    df.show(false)
    df.printSchema()

    val df2 = df.withColumn(
      "languages",
      slice(col("languagesAtSchool"), 2, 3)
    ).drop("languagesAtSchool")

    df2.printSchema()
    df2.show(false)

    df.createOrReplaceTempView("person")
    spark.sql("select name, slice(languagesAtSchool, 2, 3) as nameArray from person").show(false)
  }
}
