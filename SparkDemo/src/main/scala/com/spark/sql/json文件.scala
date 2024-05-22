package com.spark.sql

import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object json文件 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("json").master("local[*]").getOrCreate()
    // 指定字段类型
    val schema: StructType = StructType(Seq(
      StructField("id", DataTypes.LongType),
      StructField("info", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType))
    ))
    val df: DataFrame = spark.read.schema(schema).json("data/people.json")
    df.createTempView("t")

    spark.sql(
      """
        |select
        | info['age']
        |from t
        |
        |""".stripMargin).show(11, false) //不截断

    spark.close()
  }

}
