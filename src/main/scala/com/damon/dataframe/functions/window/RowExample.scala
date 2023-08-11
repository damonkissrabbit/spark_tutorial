package com.damon.dataframe.functions.window

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object RowExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("RowExample")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val simpleData = Seq(
      ("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)
    )

    val df = simpleData.toDF("employee_name", "department", "salary")
    df.show()

    // 窗口函数在使用之前需要使用Window函数来定义窗口规范
    val windowSpec = Window.partitionBy("department").orderBy("salary")

    // row_number 为结果集中每一行分配一个唯一的行号
    df.withColumn("row_number", row_number.over(windowSpec)).show(false)

  }
}
