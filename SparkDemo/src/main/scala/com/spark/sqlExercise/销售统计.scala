package com.spark.sqlExercise

import org.apache.spark.sql.{DataFrame, SparkSession}

object 销售统计 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("销售统计").master("local[*]").getOrCreate()
    val df: DataFrame = spark.read.csv("data/.csv")
    df.createTempView("t")
    df.printSchema() //打印出元数据
    //计算每个店铺每个月的销售额，以及截止到当前月的总金额
    spark.sql(
      """
        |select
        | id,
        | substr(dt,0,7) as month,
        | sum(amount),
        | sum(sum(amount)) over(partition by id,month order by substr(dt,0,7) rows between unbounded preceding and current row)
        |from t
        |group by id,substr(dt,0,7)
        |
        |""".stripMargin)
    spark.close()
  }
}
