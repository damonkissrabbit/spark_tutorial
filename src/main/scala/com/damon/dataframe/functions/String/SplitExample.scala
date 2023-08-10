package com.damon.dataframe.functions.String

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SplitExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .appName("SplitExample")
        .master("local[*]")
        .getOrCreate()

    val data = Seq(
        ("James, A, Smith","2018","M",3000),
        ("Michael, Rose, Jones","2010","M",4000),
        ("Robert,K,Williams","2010","M",4000),
        ("Maria,Anne,Jones","2005","F",4000),
        ("Jen,Mary,Brown","2010","",-1)
    )

    import spark.implicits._
    val df = data.toDF("name", "dob_year", "gender", "salary")
    df.printSchema()
    df.show(false)

    df.select(
        split($"name", ",").getItem(0).alias("firstName"),
        split(col("name"), ",").getItem(1).alias("middleName"),
        split($"name", ",").getItem(2).alias("lastName")
    ).show(false)

    df.withColumn(
        "firstName", split($"name", ",").getItem(0)
    ).withColumn(
        "middleName", split($"name", ",").getItem(1)
    ).withColumn(
        "lastName", split($"name", ",").getItem(2)
    ).select($"firstName", $"middleName", $"lastName").show(false)

    df.createOrReplaceTempView("person")

    spark.sql("select split(name, ',') as nameArray from person").show(false)

    // array_join算子将 array数组中的每一个元素用delimiter拼接起来
    df.withColumn(
        "firstName", split($"name", ",").getItem(0)
    ).withColumn(
        "middleName", array_join(slice(split($"name", ","), 2, 3), "/")
    ).withColumn(
        "nameArray", split($"name", ",")
    ).select($"firstName", $"middleName", $"nameArray").show(false)
  }
}
