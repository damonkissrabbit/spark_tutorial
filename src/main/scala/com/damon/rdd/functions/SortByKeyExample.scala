package com.damon.rdd.functions

import org.apache.spark.sql.SparkSession

object SortByKeyExample {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val data = Seq(
      ("Project", "A", 1),
      ("Gutenberg’s", "X", 3),
      ("Alice’s", "C", 5),
      ("Adventures", "B", 1)
    )

    // sortBy 可以指定 tuple 进行排序，依次按tuple里面元素的顺序进行排序
    spark.sparkContext.parallelize(data)
      .sortBy(data => (data._3, data._2)).foreach(println)
  }
}
