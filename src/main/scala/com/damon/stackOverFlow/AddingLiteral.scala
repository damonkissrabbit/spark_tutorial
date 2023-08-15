package com.damon.stackOverFlow

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._


case class Employee(EmpId: String, Experience: Double, Salary: Double)

case class Employee2(EmpId: EmpData, Experience: EmpData, Salary: EmpData)
case class EmpData(key: String, value: String)

object AddingLiteral {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate();
    import spark.sqlContext.implicits._

    val df = Seq(
      ("111",5,50000),("222",6,60000),("333",7,60000)
    ).toDF("EmpId", "Experience", "Salary")

    df.withColumn(
      "EmpId", struct(lit("1").as("key"), $"EmpId".as("value"))
    ).withColumn(
      "Experience", struct(lit("2").as("key"), $"Experience".as("value"))
    ).withColumn(
      "Salary", struct(lit("3").as("key"), $"Salary".as("value"))
    ).show(false)

    val ds = df.as[Employee]
    ds.show(false)

//    val newDS: Dataset[(EmpData, EmpData, EmpData)] = ds.map(data => {
//      (EmpData("1", data.EmpId), EmpData("2", data.Experience.toString), EmpData("3", data.Salary.toString))
//    })
//    val finalDS = newDS.as[Employee2]

    val newDS: Dataset[Employee2] = ds.map(data => {
      Employee2(EmpData("1", data.EmpId), EmpData("2", data.Experience.toString), EmpData("3", data.Salary.toString))
    })

    newDS.show(false)
  }
}
