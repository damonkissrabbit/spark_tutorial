package com.damon.dataframe.examples

import org.apache.spark.sql.SparkSession

object ReadORCFile {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val data = Seq(
      ("James ", "", "Smith", "36636", "M", 3000),
      ("Michael ", "Rose", "", "40288", "M", 4000),
      ("Robert ", "", "Williams", "42114", "M", 4000),
      ("Maria ", "Anne", "Jones", "39192", "F", 4000),
      ("Jen", "Mary", "Brown", "", "F", -1)
    )
    val columns = Seq("firstname", "middlename", "lastname", "dob", "gender", "salary")
    // columns: _* 将columns序列中的元素展开为多个参数
    // columns: _* 这个语法叫可变参数展开
    val df = spark.createDataFrame(data).toDF(columns: _*)

    df.write.mode("overwrite")
      .orc("src/main/TempFiles/orc/data.orc")

    df.write.mode("overwrite")
      .option("compression", "none")
      .orc("src/main/TempFiles/orc/data-nocomp.orc")

    df.write.mode("overwrite")
      .option("compression", "zlib")
      .orc("src/main/TempFiles/orc/data-zlib.orc")

    val df2 = spark.read.orc("src/main/TempFiles/orc/data.orc")
    df2.show(false)

    df2.createOrReplaceTempView("ORCTable")

    spark.sql("select firstname, dob from ORCTable where salary >= 4000").show(false)
    spark.sql("create temporary view person using orc options (path \"src/main/TempFiles/orc/data.orc\")")
    spark.sql("select * from PERSON").show(false)
  }
}
