package com.damon.externalDataSource

import org.apache.spark.sql.SparkSession

object S3SourceAndSink {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", "AKIA3EEY5YUYIGIE4JSQJZU")
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", "BhhNlJwGyVWCjnjuVQA1sdfsdf6wYbpzi6Myg5XxURv8lW")
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    val data = Seq(("James", "Rose", "Smith", "36636", "M", 3000),
      ("Michael", "Rose", "", "40288", "M", 4000),
      ("Robert", "Mary", "Williams", "42114", "M", 4000),
      ("Maria", "Anne", "Jones", "39192", "F", 4000),
      ("Jen", "Mary", "Brown", "1234", "F", -1)
    )

    val columns = Seq("firstname", "middlename", "lastname", "dob", "gender", "salary")
    import spark.sqlContext.implicits._
    val df = data.toDF(columns: _*)

    df.show()
    df.printSchema()

    df.write
      .parquet("s3a://sparkbyexamples/parquet/people.parquet")


    val parqDF = spark.read.parquet("s3a://sparkbyexamples/parquet/people.parquet")
    parqDF.createOrReplaceTempView("ParquetTable")

    spark.sql("select * from ParquetTable where salary >= 4000").explain()
    val parkSQL = spark.sql("select * from ParquetTable where salary >= 4000 ")

    parkSQL.show()
    parkSQL.printSchema()

    df.write
      .partitionBy("gender", "salary")
      .parquet("s3a://sparkbyexamples/parquet/people2.parquet")
  }
}
