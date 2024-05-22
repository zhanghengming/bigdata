package com.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object 非结构化类型 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("json").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("warn")
    // 非结构化的数据读出来 一行就是一个字段
    val df: DataFrame = spark.read.text("data/a.txt")
    df.createTempView("t")
    df.show()
    df.printSchema()
    // sql实现word count
    spark.sql(
      """
        |select word,count(1) from
        |(
        |select
        | split(value,' '),word
        |from t lateral view explode(split(value,' ')) t1 as word) a
        |group by word
        |
        |""".stripMargin).show(false)

    spark.close()
  }
}
