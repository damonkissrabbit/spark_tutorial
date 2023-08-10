package com.damon.dataframe.functions.collections

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object ArrayTypeExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ArrayTypeExample")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      Row("James,,Smith", List("Java", "Scala", "C++"), List("Spark", "Java"), "OH", "CA"),
      Row("Michael,Rose,", List("Spark", "Java", "C++"), List("Spark", "Java"), "NY", "NJ"),
      Row("Robert,,Williams", List("CSharp", "VB"), List("Spark", "Python"), "UT", "NV")
    )

    val schema = new StructType()
      .add("name", StringType)
      .add("languagesAtSchool", ArrayType(StringType))
      .add("languagesAtWork", ArrayType(StringType))
      .add("currentState", StringType)
      .add("previousState", StringType)

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data), schema
    )
    df.printSchema()
    df.show()

    df.select($"name", explode($"languagesAtSchool")).show(false)

    df.select(split(col("name"), ",").as("nameAsArray")).show(false)

    df.select(col("name"), array(col("currentState"), col("previousState")).as("states")).show(false)

    df.select(col("name"), array_contains(col("languagesAtSchool"), "java").as("array_contains")).show(false)

    // array_union 会进行去重
    df.select(col("name"), array_union(col("languagesAtSchool"), col("languagesAtWork")).as("array_union")).show(false)

    df.select(col("name"), array_intersect(col("languagesAtSchool"), col("languagesAtWork")).alias("array_intersect")).show(false)

    df.select(col("name"), array_except(col("languagesAtSchool"), col("languagesAtWork")),
      array_except(col("languagesAtWork"), col("languagesAtSchool"))).show(false)

    df.select(col("name"), array_join(col("languagesAtWork"), "|")).show(false)

    df.select(col("name"), array_repeat(col("languagesAtWork"), 3).alias("array_repeat")).show(false)

    df.select(col("name"), array_remove(col("languagesAtWork"), "Java").alias("array_repeat")).show(false)

    df.select(col("name"), array_sort(col("languagesAtWork")).alias("array_sort")).show(false)

    // arrays_overlap 检查两个array是否有重复元素
    df.select(col("name"), arrays_overlap(col("languagesAtSchool"), col("languagesAtWork")).alias("array_overlap")).show(false)

    // 类似于scala array的zip操作
    df.select(col("name"), arrays_zip(col("languagesAtSchool"), col("languagesAtWork")).alias("arrays_zip")).show(false)

    // 连接两个array，concat不会进行去重
    df.select(col("name"), concat(col("languagesAtSchool"), col("languagesAtWork")).alias("concat")).show(false)

    df.select(col("name"), array_distinct(concat(col("languagesAtSchool"), col("languagesAtWork"))).alias("array_distinct_concat")).show(false)

    df.select(col("name"), col("languagesAtSchool"), reverse(col("languagesAtSchool").alias("reverse"))).show(false)

    // element_at 用户获取数组或者map指定位置的索引的元素
    df.select(col("name"), element_at(col("languagesAtSchool"), 3).alias("element_as"), col("languagesAtSchool").getItem(2)).show(false)

    df.select(col("name"), sequence(lit(1).cast("int"), lit(10).cast("int")).alias("sequence")).show(false)

    df.select(col("name"), shuffle(col("languagesAtSchool")).alias("shuffle")).show(false)

    df.select(col("name"), slice(col("languagesAtSchool"), 3, 2).alias("slice")).show(false)

    val arrayArrayData = Seq(
      Row("James",List(List("Java","Scala","C++"),List("Spark","Java"),List("Spark","Java"))),
      Row("Michael",List(List("Spark","Java","C++"),List("Spark","Java"))),
      Row("Robert",List(List("CSharp","VB"),List("Spark","Python")))
    )

    val arrayArraySchema = new StructType()
      .add("name", StringType)
      .add("languages", ArrayType(ArrayType(StringType)))

    spark.createDataFrame(
      spark.sparkContext.parallelize(arrayArrayData), arrayArraySchema
    ).select($"name", flatten($"languages").alias("flatten"), explode($"languages").alias("explode")).show(false)

    // posexplode 就是在爆炸的时候加上了 pos
    spark.createDataFrame(
      spark.sparkContext.parallelize(arrayArrayData), arrayArraySchema
    ).select($"name", posexplode($"languages")).show(false)


    val simpleData = Seq(("James","Sales",3000),
      ("Michael","Sales",4600),
      ("Robert","Sales",4100),
      ("Maria","Finance",3000),
      ("Jen","Finance",3000),
      ("Jen","Finance",3300),
      ("Jen","Finance",3900),
      ("Jen","Marketing",3000),
      ("Jen","Marketing",2000)
    )

    simpleData.toDF("name", "department", "salary")
      .groupBy($"department")
      .agg(
        collect_list($"name").alias("collect_list"),
        collect_set(($"name")).alias("collect_set"),
        sum($"salary").alias("sum_salary"),
        max($"salary").alias("max_salary"),
        min($"salary").alias("min_salary")
    ).show(false)

    simpleData.toDF("name", "department", "salary")
      .re
  }
}
