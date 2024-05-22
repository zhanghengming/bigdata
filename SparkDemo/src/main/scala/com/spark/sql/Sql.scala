package com.spark.sql

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Sql {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("sparkSql").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("warn")
    import spark.implicits._
    // 定义字段类型
    //val schema = "empno string,ename string,job string,mgr string ,hiredate date,sal double,comm double,deptno int"
    // 方式一
    val schema1: StructType = (new StructType)
      // false 说明字段不能为空
      .add("empno", "string", false)
      .add("ename", "string", false)
      .add("job", "string", false)
      .add("mgr", "string", false)
      .add("hiredate", "date", false)
      .add("sal", "double", false)
      .add("comm", "int", false)
      .add("deptno", "int", false)
    val df1: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .schema(schema1)
      .load("data/emp.dat")
    // 方式二
    val df2: DataFrame = spark.read.format("csv")
      .option("header", "true")
      // 分割符
      .option("seq", ",")
      // 自动推断类型
      .option("inferschema", "true")
      .load("data/emp.dat")

    df1.createOrReplaceTempView("t1")
    val result: DataFrame = spark.sql(
      """
        |select * from t1 where mgr is not null and comm is not null
        |""".stripMargin)

    result.show()

    spark.close()
  }
}
