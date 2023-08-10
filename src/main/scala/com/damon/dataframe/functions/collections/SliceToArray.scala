package com.damon.dataframe.functions.collections

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

object SliceToArray {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkByExamples.com")
      .master("local")
      .getOrCreate()

    val data = Seq(("James, A, Smith","2018","M",3000),
      ("Michael, Rose, Jones","2010","M",4000),
      ("Robert,K,Williams","2010","M",4000),
      ("Maria,Anne,Jones","2005","F",4000),
      ("Jen,Mary,Brown","2010","",-1)
    )

    import spark.sqlContext.implicits._
    val df = data.toDF("name","dob_year","gender","salary")
    df.printSchema()
    df.show(false)

    df.select(split(col("name"), ",").alias("nameArray")).drop("name").show(false)

    df.createOrReplaceTempView("person")
    spark.sql("select split(name, ',') as nameArray from person").show(false)
  }
}
