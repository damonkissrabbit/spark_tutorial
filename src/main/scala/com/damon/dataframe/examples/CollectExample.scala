package com.damon.dataframe.examples

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object CollectExample {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val data = Seq(Row(Row("James","","Smith"),"36636","M",3000),
      Row(Row("Michael","Rose",""),"40288","M",4000),
      Row(Row("Robert","","Williams"),"42114","M",4000),
      Row(Row("Maria","Anne","Jones"),"39192","F",4000),
      Row(Row("Jen","Mary","Brown"),"","F",-1)
    )

    val schema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("id",StringType)
      .add("gender",StringType)
      .add("salary",IntegerType)

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df.printSchema()
    df.show()

    val colList = df.collectAsList()
    println(colList)

    val colData = df.collect()

    colData.foreach(data => {
      val salary = data.getInt(3)
      println(salary)
    })

    colData.foreach(row => {
      val salary = row.getInt(3)
      val fullName = row.getStruct(0)
      val firstName = fullName.getString(0)
      val middleName = fullName.getString(1)
      val lastName = fullName.getString(2)
      println(firstName + "," + middleName + "," + lastName)
    })

  }
}
