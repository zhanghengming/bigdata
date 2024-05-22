package com.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

import java.lang
case class Person(name:String,age:Int,height:Int)

object SparkSql {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("Spark Sql")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")
    import spark.implicits._

    val numDS: Dataset[lang.Long] = spark.range(5, 100, 5)
    //  orderBy 转换操作；desc：function；
    numDS.orderBy(desc("id")).show
    // 统计信息
    numDS.describe().show()
    // 显示schema信息
    numDS.printSchema
    // 使用RDD执行同样的操作
    println(numDS.rdd.map(_.toInt).stats)
    // 检查分区数
    println(numDS.rdd.getNumPartitions)

    // 由集合生成DS
    val seq1 = Seq(Person("zhang",28,123),Person("shan",25,180))
    val ds1: Dataset[Person] = spark.createDataset(seq1)
    ds1.printSchema()
    ds1.show()

    val seq2 = Seq(("Jack", 28, 184), ("Tom", 10, 144), ("Andy", 16, 165))
    val ds2: Dataset[(String, Int, Int)] = spark.createDataset(seq2)
    ds2.show()
    spark.close()
  }
}
