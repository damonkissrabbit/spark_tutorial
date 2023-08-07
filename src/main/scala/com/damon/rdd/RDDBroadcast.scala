package com.damon.rdd

import org.apache.spark.sql.SparkSession

object RDDBroadcast {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .appName("RDDBroadcast")
        .master("local[*]")
        .getOrCreate()
    
    val states = Map(
        ("NY","New York"),
        ("CA","California"),
        ("FL","Florida")
    )

    val countries = Map(
        ("USA","United States of America"),
        ("IN","India")
    )

    val boradcastStates = spark
        .sparkContext
        .broadcast(states)
    
    val broadcastCountries = spark
        .sparkContext
        .broadcast(countries)

    val data = Seq(
        ("James","Smith","USA","CA"),
        ("Michael","Rose","USA","NY"),
        ("Robert","Williams","USA","CA"),
        ("Maria","Jones","USA","FL")
    )

    val rdd = spark.sparkContext.parallelize(data)

    val rdd2 = rdd.map(data => {
        val country = data._3
        val state = data._4
        val fullCountry = broadcastCountries.value.get(country).get
        val fullState = boradcastStates.value.get(state).get
        (data._1, data._2, fullCountry, fullState)
    })

    println(rdd2.collect().mkString("\n"))
  }
}
