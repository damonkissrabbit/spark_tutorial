package com.damon.dataframe.functions.collections

import org.apache.spark.sql.functions.{col, explode, flatten}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object ArrayOfArrayType {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkByExamples.com")
      .master("local[1]")
      .getOrCreate()

    val arrayArrayData = Seq(
      Row("James",List(List("Java","Scala","C++"),List("Spark","Java"))),
      Row("Michael",List(List("Spark","Java","C++"),List("Spark","Java"))),
      Row("Robert",List(List("CSharp","VB"),List("Spark","Python")))
    )

    val schema = new StructType()
      .add("name", StringType)
      .add("subjects", ArrayType(ArrayType(StringType)))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayArrayData), schema
    )
    df.printSchema()
    df.show(false)

    import spark.implicits._
    df.select($"name", explode($"subjects")).show()

    df.select(col("name"), explode(col("subjects"))).show()

    df.select(col("name"), flatten(col("subjects"))).show(false)

  }
}
