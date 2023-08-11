package com.damon.dataframe.functions.window

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowFunctions {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("WindowFunctions")
      .master("local[*]")
      .getOrCreate()

    // 导入 import spark.implicits._ 的目的是为了引入 Spark 的隐式转换函数，
    // 从而使你能够更方便地在 DataFrame 和 Dataset 上使用各种操作和方法。
    // 这些隐式转换函数可以将原生的 Scala 数据类型转换为 Spark 的数据类型，
    // 使得你可以像操作普通 Scala 集合一样来操作 Spark 的分布式数据集
    import spark.implicits._

    val simpleData = Seq(("James", "Sales", 3000),
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

    // 1, 2, 3
    val windowSpec = Window.partitionBy("department").orderBy("salary")
    df.withColumn(
      "row_number", row_number().over(windowSpec)
    ).select("*").show(false)

    // 1, 1, 3
    df.withColumn(
      "rank", rank().over(windowSpec)
    ).show(false)

    // 1, 1, 2
    df.withColumn(
      "dense_rank", dense_rank().over(windowSpec)
    ).show(false)

    df.withColumn(
      "percent_rank", percent_rank().over(windowSpec)
    ).show(false)

    df.withColumn(
      "ntile", ntile(3).over(windowSpec)
    ).show(false)

    df.withColumn(
      "cume_dist", cume_dist().over(windowSpec)
    ).show(false)

    df.withColumn(
      "lag", lag("salary", 2).over(windowSpec)
    ).show(false)

    df.withColumn(
      "lead", lead("salary", 2).over(windowSpec)
    ).show(false)

    val windowSpecAgg = Window.partitionBy("department")

    df.withColumn(
      "row", row_number().over(windowSpec)
    ).withColumn(
      "avg", avg($"salary").over(windowSpecAgg)
    ).withColumn(
      "sum", sum($"salary").over(windowSpecAgg)
    ).withColumn(
      "max", max($"salary").over(windowSpecAgg)
    ).withColumn(
      "min", min($"salary").over(windowSpecAgg)
    ).where($"row" === 1).show(false)

  }
}
