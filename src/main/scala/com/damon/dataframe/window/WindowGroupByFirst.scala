package com.damon.dataframe.window

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowGroupByFirst {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .appName("WindowGroupByFirst")
        .master("local[*]")
        .getOrCreate()
    
    val simpleData = Seq(
        ("James","Sales",3000),
        ("Michael","Sales",4600),
        ("Robert","Sales",4100),
        ("Maria","Finance",3000),
        ("Raman","Finance",3000),
        ("Scott","Finance",3300),
        ("Jen","Finance",3900),
        ("Jeff","Marketing",3000),
        ("Kumar","Marketing",2000)
    )

    // implicit conversion 隐式转换
    // implicit 隐含的，含蓄的，盲从的
    import spark.implicits._

    val df = simpleData.toDF("employee_name","department","salary")
    df.show()

    val w2 = Window.partitionBy("department").orderBy($"salary")
    df.withColumn(
        "row", row_number.over(w2)
    ).where($"row" === 1).show(false)

    val windowSpec2 = Window.partitionBy("department").orderBy($"salary".desc)
    df.withColumn(
        "row", row_number().over(windowSpec2)
    ).where($"row" === 1).show(false)

    // Maximum, Minimum, Average, salary for each window
    val windowSpec3 = Window.partitionBy("department")

    // 求出 row===1 是在windowSpec2窗口上做的，这个窗口窗口上先根据department分区，然后再根据salary做降序排序
    // avg, sum, min, max 则是在 windowSpec3 窗口上做的，这个窗口只做按照department分区
    // 所以计算出来的这个同一个分区里面所有数据的 avg, sum, min, max
    df.withColumn(
        "row", row_number().over(windowSpec2)
    ).withColumn(
        "avg", avg($"salary").over(windowSpec3)
    ).withColumn(
        "sum", sum($"salary").over(windowSpec3)
    ).withColumn(
        "min", min($"salary").over(windowSpec3)
    ).withColumn(
        "max", max($"salary").over(windowSpec3)
    ).where($"row" === 1)
    .select("department", "avg", "sum", "min", "max").show(false)
  }
}
