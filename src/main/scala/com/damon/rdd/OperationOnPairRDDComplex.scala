package com.damon.rdd

import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object OperationOnPairRDDComplex {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("OperationOnPairRDDComplex")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D")

    val data = spark.sparkContext.parallelize(keysWithValuesList)

    val kv = data
      .map(_.split("="))
      .map(data => (data(0), data(1)))
      .cache()

    kv.foreach(println)

    val se = mutable.HashSet.empty[String]

    def param3 = (accu: mutable.HashSet[String], v: String) => accu + v

    def param4 = (acc1: mutable.HashSet[String], acc2: mutable.HashSet[String]) => acc1 ++= acc2

    // param3 是一个局部聚合函数，接受两个参数：一个是累加器，一个是当前分区中的值，该函数在每个分区中将局部值聚合到累加器
    // param3 是一个全局聚合函数，接受两个参数：两个累加器，该函数在所有分区的局部聚合结果上执行最终的全局聚合操作
    kv.aggregateByKey(se)(param3, param4).foreach(println)

    def param5 = (accu: Int, v: String) => accu + 1

    def param6 = (accu1: Int, accu2: Int) => accu1 + accu2

    kv.aggregateByKey(0)(param5, param6).foreach(println)

    val studentRDD = spark.sparkContext.parallelize(
      Array(
        ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91), ("Joseph", "Biology", 82),
        ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62), ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80),
        ("Tina", "Maths", 78), ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),
        ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91), ("Thomas", "Biology", 74),
        ("Cory", "Maths", 56), ("Cory", "Physics", 65), ("Cory", "Chemistry", 71), ("Cory", "Biology", 68),
        ("Jackeline", "Maths", 86), ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75), ("Jackeline", "Biology", 83),
        ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64), ("Juan", "Biology", 60)
      ),
      3
    )

    println("*************************************************")

    // 进入reduceByKey算子里面的数据时每个相同key的值
    studentRDD
      .map(data=> (data._1, (data._2, data._3)))
      .reduceByKey((accu1, accu2) => {
        (accu1._1, if (accu1._2 > accu2._2) accu1._2 else accu2._2)
      })
      .foreach(println)

    val def1 = (accu: Int, v: (String, Int)) => if (accu > v._2) accu else v._2
    val def2 = (accu1: Int, accu2: Int) => if (accu1 > accu2) accu1 else accu2

    println("*************************************************")

    studentRDD
      .map(data => (data._1, (data._2, data._3)))
      .aggregateByKey(0)(def1, def2).foreach(println)


    val zeroval = ("", 0)
    val def3 = (accu: (String, Int), v: (String, Int)) => if (accu._2 > v._2) accu else v
    val def4 = (accu1: (String, Int), accu2: (String, Int)) => if (accu1._2 > accu2._2) accu1 else accu2

    println("*************************************************")

    studentRDD
      .map(data => (data._1, (data._2, data._3)))
      .aggregateByKey(zeroval)(def3, def4).foreach(println)

    val def5 = (accu: Int, v: (String, Int)) => accu + v._2
    val def6 = (accu1: Int, accu2: Int) => accu1 + accu2

    println("*************************************************")

    val studentScoreTotals = studentRDD
      .map(data => (data._1, (data._2, data._3)))
      .aggregateByKey(0)(def5, def6)

    studentScoreTotals.foreach(println)

    val tot = studentScoreTotals.max()(new Ordering[(String, Int)](){
      override def compare(x: (String, Int), y: (String, Int)): Int = {
        Ordering[Int].compare(x._2, y._2)
      }
    })
    println("First class student : " + tot._1 + " = " + tot._2)



  }
}
