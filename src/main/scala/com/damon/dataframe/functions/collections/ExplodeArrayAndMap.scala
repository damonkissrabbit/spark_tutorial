package com.damon.dataframe.functions.collections

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object ExplodeArrayAndMap {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ExplodeArrayAndMap")
      .master("local[*]")
      .getOrCreate()

    val arrayData = Seq(
      Row("James", List("Java", "Scala"), Map("hair" -> "black", "eye" -> "brown")),
      Row("Michael", List("Spark", "Java", null), Map("hair" -> "brown", "eye" -> null)),
      Row("Robert", List("CSharp", ""), Map("hair" -> "red", "eye" -> "")),
      Row("Washington", null, null),
      Row("Jeferson", List(), Map())
    )

    val schema = new StructType()
      .add("name", StringType)
      .add("knownLanguages", ArrayType(StringType))
      .add("properties", MapType(StringType, StringType))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayData), schema
    )
    df.show(false)
    import spark.implicits._

    //explode
    df.select($"name", explode($"knownLanguages").alias("explode")).show(false)

    // explode_outer 爆炸后，保留了null数据
    df.select($"name", explode_outer($"knownLanguages").alias("explode_outer")).show(false)

    // posexplode 爆炸后，加上了pos
    df.select($"name", posexplode($"knownLanguages")).show(false)

    // posexplode_outer 爆炸后，加上了pos，也保留了null数据
    df.select($"name", posexplode_outer($"knownLanguages")).show(false)

    val arrayStructData = Seq(
      Row("James", List(Row("Java", "XX", 120), Row("Scala", "XA", 300))),
      Row("Michael", List(Row("Java", "XY", 200), Row("Scala", "XB", 500))),
      Row("Robert", List(Row("Java", "XZ", 400), Row("Scala", "XC", 250))),
      Row("Washington", null)
    )

    val arrayStructSchema = new StructType().add("name", StringType)
      .add("booksIntersted",
        ArrayType(new StructType()
          .add("name", StringType)
          .add("author", StringType)
          .add("pages", IntegerType)
        )
      )


    val df3 = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructData), arrayStructSchema
    )

    df3.printSchema()
    df3.show()

    df3.select($"name", explode($"booksIntersted")).show(false)

    val arrayMapSchema = new StructType().add("name", StringType)
      .add("properties", ArrayType(new MapType(StringType, StringType, true)))

    val arrayMapData = Seq(
      Row("James", List(Map("hair" -> "black", "eye" -> "brown"), Map("height" -> "5.9"))),
      Row("Michael", List(Map("hair" -> "brown", "eye" -> "black"), Map("height" -> "6"))),
      Row("Robert", List(Map("hair" -> "red", "eye" -> "gray"), Map("height" -> "6.3")))
    )

    val df5 = spark.createDataFrame(spark.sparkContext.parallelize(arrayMapData), arrayMapSchema)
    df5.printSchema()
    df5.show()

    df5.select($"name", explode($"properties"))
      .show(false)

    val arrayArrayData = Seq(
      Row("James", List(List("Java", "Scala", "C++"), List("Spark", "Java"))),
      Row("Michael", List(List("Spark", "Java", "C++"), List("Spark", "Java"))),
      Row("Robert", List(List("CSharp", "VB"), List("Spark", "Python")))
    )

    val arrayArraySchema = new StructType().add("name", StringType)
      .add("subjects", ArrayType(ArrayType(StringType)))

    val df4 = spark.createDataFrame(spark.sparkContext.parallelize(arrayArrayData), arrayArraySchema)
    df4.printSchema()
    df4.show()


    df4.select($"name", explode($"subjects"))
      .select($"name", explode($"col"))
      .show(false)

    df4.select($"name", flatten($"subjects")).show(false)

    df4.withColumn("flatten", flatten($"subjects")).select($"name", explode($"flatten")).show(false)
  }
}


