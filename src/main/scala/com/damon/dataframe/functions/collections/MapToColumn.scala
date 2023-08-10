package com.damon.dataframe.functions.collections

import io.netty.handler.codec.smtp.SmtpRequests.data
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object MapToColumn {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val arrayStructureData = Seq(
      Row("James",Map("hair"->"black","eye"->"brown")),
      Row("Michael",Map("hair"->"gray","eye"->"black")),
      Row("Robert",Map("hair"->"brown"))
    )

    val mapType  = DataTypes.createMapType(StringType,StringType)

    val arrayStructureSchema = new StructType()
      .add("name", StringType)
      .add("property", MapType(StringType, StringType))

    val mapTypeDF = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData), arrayStructureSchema
    )

    mapTypeDF.show(false)

    mapTypeDF.select(col("name"), col("property").getItem("hair").as("hair_color"), col("property").getItem("eye").as("eye_color")).show(false)

    import spark.implicits._
    val keysDF = mapTypeDF.select(explode(map_keys(col("property")))).distinct()
    keysDF.show(false)
    // collect 之后出的数据时一个 array, array里面每一个数据是每一行数据组成的一个array
    val keys = keysDF.collect().map(f => f.get(0))
    val keysCols = keys.map(f => col("property").getItem(f).as(f.toString))

    mapTypeDF.select($"name" +: keysCols:_*).collect().foreach(println)
  }
}
