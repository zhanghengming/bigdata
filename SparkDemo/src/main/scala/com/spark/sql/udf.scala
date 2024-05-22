package com.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DataTypes, IntegerType, StructField, StructType}

import scala.collection.mutable

// 给定样本数据去推测相似度
object udf {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder.appName("udf").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("warn")
    // 没有表头，需要指定schema
    val schema = StructType(Seq(
      StructField("id", DataTypes.IntegerType),
      StructField("f1", DataTypes.IntegerType),
      StructField("f2", DataTypes.IntegerType),
      StructField("f3", DataTypes.IntegerType),
      StructField("gender", DataTypes.StringType)
    ))
    val df1: DataFrame = spark.read.schema(schema).csv("data/相似度推测/sample.txt")

    val schema1 = StructType(Seq(
      StructField("id", DataTypes.IntegerType),
      StructField("f1", DataTypes.IntegerType),
      StructField("f2", DataTypes.IntegerType),
      StructField("f3", DataTypes.IntegerType),
      StructField("gender", DataTypes.StringType)
    ))
    val df2: DataFrame =spark.read.schema(schema1).csv("data/相似度推测/test.txt")

    df1.createTempView("sample")
    df2.createTempView("test")

    // 在Spark SQL中，array函数返回的数组类型是WrappedArray ,使用Array[Int]会报错，所以需要使用mutable.WrappedArray[Int]
    val dist = (arr1: mutable.WrappedArray[Int], arr2: mutable.WrappedArray[Int]) => {
      arr1.zip(arr2).map(x => Math.pow(x._1 - x._2, 2)).sum
    }

    // 注册udf
    spark.udf.register("dist", dist)



    // todo ： 实现相似度推测算法，并注册为udf，参数是样本数据集和待推测数据集数组类型
    // knn算法，计算待推测数据集和每个样本数据集的距离，然后取出最小的距离就是最相似的样本
    spark.sql(
      """
        |select
        |id,
        |sample_gender
        |from(
        | select
        |  a.id,
        |  sample_gender,
        |  row_number() over (partition by id order by dist asc) as rank
        |  from
        |  (select
        |    test.id,
        |    sample.gender as sample_gender,
        |    test.gender as test_gender,
        |    dist(array(sample.f1, sample.f2, sample.f3), array(test.f1, test.f2, test.f3)) as dist
        |  from sample cross join test
        |  ) a
        | ) b
        |where rank = 1
        |""".stripMargin).show(100, truncate = false)

    spark.close()
  }
}
