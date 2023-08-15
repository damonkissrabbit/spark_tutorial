package com.damon.dataframe.functions

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.generic.IdleSignalling.tag

object PivotDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("pivotDemo")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val data = Seq(
      (1, "US", 50),
      (1, "UK", 100),
      (1, "Can", 125),
      (2, "US", 75),
      (2, "UK", 150),
      (2, "Can", 175),
    )

    val df = data.toDF("id", "tag", "value")
    df.show(false)

    val countries = List("US", "UK", "Can")

    // 怎么写udf
    val countryValue = udf { (countryToCheck: String, countryInRow: String, value: Long) =>
      if (countryToCheck == countryInRow) value else 0
    }

    // Function.chain 接受一一系列的匿名函数，countryFuncs这相当于是一个函数列表，
    // 里面(dataFrame: DataFrame) => dataFrame.withColumn(country, countryValue(lit(country), df("tag"), df("value")))是一个匿名函数
    // 然后在外层对countries列表使用里map，也就是对countries列表里面的每一个元素都与DataFrame进行结合
    // 可以通过两个DataFrame中元素来进行更好的理解
    // Function.chain 里面的函数的输入和输出类型相同

    // previous dataFrame
    // +---+---+-----+
    // |id |tag|value|
    // +---+---+-----+
    // |1  |US |50   |
    // |1  |UK |100  |
    // |1  |Can|125  |
    // |2  |US |75   |
    // |2  |UK |150  |
    // |2  |Can|175  |
    // +---+---+-----+
    // the dataFrame after use Function.chain
    //  +---+---+-----+---+---+---+
    //  |id |tag|value|US |UK |Can|
    //  +---+---+-----+---+---+---+
    //  |1  |US |50   |50 |0  |0  |
    //  |1  |UK |100  |0  |100|0  |
    //  |1  |Can|125  |0  |0  |125|
    //  |2  |US |75   |75 |0  |0  |
    //  |2  |UK |150  |0  |150|0  |
    //  |2  |Can|175  |0  |0  |175|
    //  +---+---+-----+---+---+---+

    val countryFuncs: List[DataFrame => DataFrame] = countries.map { country => (dataFrame: DataFrame) => dataFrame.withColumn(country, countryValue(lit(country), df("tag"), df("value"))) }

    // Function.chain 通过Function.chain 把一个函数列表组合成一个函数链
    val dfWithCountries = Function.chain(countryFuncs)(df).drop("tag").drop("value")
    dfWithCountries.show(false)

    dfWithCountries
      .groupBy("id")
      .sum(countries:_*).show(false)
  }
}
