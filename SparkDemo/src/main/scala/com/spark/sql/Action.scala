package com.spark.sql

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util

object Action {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("action").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("warn")
    import spark.implicits._
    val df: DataFrame = spark.read.option("header", "true")
      .option("inferschema", "true")
      .csv("data/emp.dat")
    df.count()
    // 缺省显示20行,不去重
    df.union(df).show()
    df.show(2)
    // 不截断字符
    df.toJSON.show(false)
    //spark.catalog.listFunctions.show(1000,false)

    val rows: Array[Row] = df.collect()
    val rows1: util.List[Row] = df.collectAsList()

    val row: Row = df.head()
    val row1: Row = df.first()
    val rows2: Array[Row] = df.head(3)

    val rows3: Array[Row] = df.take(3)

    val rows4: util.List[Row] = df.takeAsList(3)

//    println(rows.mkString(","))
//    println(rows1)
//    println(row)
//    println(row1)
//    println(rows2.mkString)
//    println(rows3.mkString)
//    println(rows4)

//    println(df.columns.mkString)// 查看列名
//    println(df.dtypes.mkString)// 查看列名和类型
//    println(df.explain())// 参看执行计划
//    println(df.col("ENAME"))// 获取某个列
//    df.printSchema()


    spark.close()
  }
}
