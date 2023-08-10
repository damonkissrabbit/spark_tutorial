package com.damon.dataframe.functions.collections

import io.netty.handler.codec.smtp.SmtpRequests.data
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row, SparkSession}

import scala.collection.mutable
import scala.reflect.internal.util.NoPosition.show

object MapFunctions {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    import spark.implicits._

    val structureData = Seq(
      Row("36636", "Finance", Row(3000, "USA")),
      Row("40288", "Finance", Row(5000, "IND")),
      Row("42114", "Sales", Row(3900, "USA")),
      Row("39192", "Marketing", Row(2500, "CAN")),
      Row("34534", "Sales", Row(6500, "USA"))
    )

    val structureSchema = new StructType()
      .add("id", StringType)
      .add("dept", StringType)
      .add("properties", new StructType()
        .add("salary", IntegerType)
        .add("location", StringType)
      )

    var df = spark.createDataFrame(
      spark.sparkContext.parallelize(structureData), structureSchema
    )
    df.printSchema()
    df.show(false)

    val index = df.schema.fieldIndex("properties")

    val propSchema = df.schema(index).dataType.asInstanceOf[StructType]
    val columns = mutable.LinkedHashSet[Column]()
    propSchema.fields.foreach(field => {
      columns.add(lit(field.name))
      columns.add(col("properties." + field.name))
    })

    // columns.toSeq  ArrayBuffer(salary, properties.salary, location, properties.location)
    df = df.withColumn("propertiesMap", map(columns.toSeq: _*))
    df.show(false)

    // collect 之后的数据是一个array
    println(df.select(explode(map_keys($"propertiesMap"))).as[String].collect().mkString("Array(", ", ", ")"))
  }
}
