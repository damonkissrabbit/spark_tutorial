package com.damon.dataframe.functions.aggregate

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregateFunctions {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

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

    import spark.implicits._
    val schema = Seq("employee_name", "department", "salary")
    val df = simpleData.toDF(schema:_*)
    df.show()

//    for (data <- spark.sparkContext.parallelize(simpleData).collect()) {
//      println(data)
//    }

    // approx_count_distinct()
    println("approx_count_distinct: " + df.select(approx_count_distinct("salary")).collect()(0)(0))

    // avg
    df.select(avg("salary")).show(false)

    // collect_list
    df.select(collect_list("salary")).show(false)

    // collect_set
    df.select(collect_set("salary")).show(false)

    // countDistinct
    val df2 = df.select(countDistinct("department", "salary"))
    df2.show(false)
    println("Distinct count of department & salary: " + df2.collect()(0)(0))

    // first
    println(df.select(first("salary")).collect()(0)(0))

    println(df.select(last("salary")).collect()(0)(0))

    // 用于计算数据集的峰度 kurtosis， 峰度是描述分布数据尾部相对于正态分布分布的尾部的数据统计量
    df.select(kurtosis("salary")).show(false)

    df.select(max("salary")).show()
    df.select(min("salary")).show()
    df.select(mean("salary")).show()
    // skewness 用于计算偏度，用于描述数据的不对称性，即数据分布相对于平均值的偏移程度
    df.select(skewness("salary")).show()
    // stddev 用于计算标准差                     stddev_samp 计算样本标准差      stddev_pop 总体标准差
    df.select(stddev("salary"), stddev_samp("salary"), stddev_pop("salary")).show(false)

    df.select(sum("salary")).show()
    // 计算去重后的求和函数
    df.select(sumDistinct("salary")).show()
    // variance 计算方差 var_samp              var_pop 总体方差
    df.select(variance("salary"), var_samp("salary"), var_pop("salary")).show(false)

  }
}
