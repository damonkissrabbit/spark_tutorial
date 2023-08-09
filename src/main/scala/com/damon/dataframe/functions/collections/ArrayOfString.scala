package com.damon.dataframe.functions.collections

import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object ArrayOfString {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val arrayStructureData = Seq(
      Row("James,,Smith",List("Java","Scala","C++"),"CA"),
      Row("Michael,Rose,",List("Spark","Java","C++"),"NJ"),
      Row("Robert,,Williams",List("CSharp","VB"),"NV")
    )

    val arrayStructureSchema = new StructType()
      .add("name",StringType)
      .add("languagesAtSchool", ArrayType(StringType))
      .add("currentState", StringType)

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
    df.printSchema()
    df.show()
    df.withColumn(
      "languageAtSchool", concat_ws(",", col("languagesAtSchool"))
    ).show(false)

    df.createOrReplaceTempView("ARRAY_STRING")
    spark.sql("select name concat_ws(',', languageAtSchool) as languageAtSchool, currentState from ARRAY_STRING").show(false)
  }
}
