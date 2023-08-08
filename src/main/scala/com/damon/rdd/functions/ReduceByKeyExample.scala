package com.damon.rdd.functions

import org.apache.spark.sql.SparkSession

object ReduceByKeyExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ReduceByKeyExample")
      .master("local[*]")
      .getOrCreate()

    val data = Seq(
      ("Project", 1),
      ("Gutenberg’s", 1),
      ("Alice’s", 1),
      ("Adventures", 1),
      ("in", 1),
      ("Wonderland", 1),
      ("Project", 1),
      ("Gutenberg’s", 1),
      ("Adventures", 1),
      ("in", 1),
      ("Wonderland", 1),
      ("Project", 1),
      ("Gutenberg’s", 1),
        ("Project", 1),
      ("Gutenberg’s", 1),
      ("Alice’s", 1),
      ("Adventures", 1),
      ("in", 1),
      ("Wonderland", 1),
      ("Project", 1),
      ("Gutenberg’s", 1),
      ("Adventures", 1),
      ("in", 1),
      ("Wonderland", 1),
      ("Project", 1),
      ("Gutenberg’s", 1),
      ("Gutenberg’s", 1),
      ("Alice’s", 1),
      ("Adventures", 1),
      ("in", 1),
      ("Wonderland", 1),
      ("Project", 1),
      ("Gutenberg’s", 1),
      ("Adventures", 1),
      ("in", 1),
      ("Wonderland", 1),
      ("Project", 1),
      ("Gutenberg’s", 1),
        ("Project", 1),
      ("Gutenberg’s", 1),
      ("Alice’s", 1),
      ("Adventures", 1),
      ("in", 1),
      ("Wonderland", 1),
      ("Project", 1),
      ("Gutenberg’s", 1),
      ("Adventures", 1),
      ("in", 1),
      ("Wonderland", 1),
      ("Project", 1),
      ("Gutenberg’s", 1),
        ("Project", 1),
      ("Gutenberg’s", 1),
      ("Alice’s", 1),
      ("Adventures", 1),
      ("in", 1),
      ("Wonderland", 1),
      ("Project", 1),
      ("Gutenberg’s", 1),
      ("Adventures", 1),
      ("in", 1),
      ("Wonderland", 1),
      ("Project", 1),
      ("Gutenberg’s", 1)

    )

    val rdd = spark.sparkContext.parallelize(data)

    println(rdd.getNumPartitions)
    rdd.reduceByKey(_+_).foreach(println)
  }
}
