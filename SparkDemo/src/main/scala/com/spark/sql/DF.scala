package com.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object DF {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("DF").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("warn")
    import spark.implicits._
    /**
     * 由集合生成DataFrame
     */
    val lst = List(("Jack", 28, 184), ("Tom", 10, 144), ("Andy", 16, 165))
    val df: DataFrame = spark.createDataFrame(lst)
      // 修改单个列名
      .withColumnRenamed("_1", "name")
      .withColumnRenamed("_2", "age")
      .withColumnRenamed("_3", "height")
    df.orderBy("age").show()
    df.orderBy(desc("age")).show
    // 修改整个列名
    val df1: DataFrame = spark.createDataFrame(lst).toDF("name", "age", "height")
    df1.show()

    /**
     * RDD 转成 DataFrame
     */
    val arr = Array(("Jack", 28, 184), ("Tom", 10, 144), ("Andy", 16, 165))
    val rdd1: RDD[Row] = spark.sparkContext.makeRDD(arr).map(f => Row(f._1, f._2, f._3))
    val schema: StructType = (new StructType)
      // false 说明字段不能为空
      .add("name", "string", false)
      .add("age", "int", false)
      .add("height", "int", false)
    // RDD => DataFrame，要指明schema和rdd
    val rddToDf: DataFrame = spark.createDataFrame(rdd1, schema)
    rddToDf.orderBy(desc("name")).show(false)

    val arr2 = Array(("Jack", 28, 184), ("Tom", 10, 144), ("Andy", 16, 165))
    val rdd2: RDD[Person] = spark.sparkContext.makeRDD(arr2).map(f => Person(f._1, f._2, f._3))
    // 反射推断 通过rdd转ds或df时，rdd的类型必须是case class 或者是元组
    val df11: DataFrame = rdd2.toDF()
    // 反射推断，spark 通过反射从case class的定义得到类名
    val ds1: Dataset[Person] = rdd2.toDS()

    df11.printSchema()
    ds1.printSchema()
    df11.orderBy(desc("name")).show(10)
    ds1.orderBy(desc("name")).show(10)

    /**
     * 文件创建df
     */
    val dataFrame1: DataFrame = spark.read.csv("data/people1.csv")
    dataFrame1.printSchema()
    dataFrame1.show()
    // 自动类型推断 简单类型可以
    val dataFrame2: DataFrame = spark.read.option("delimiter", ",")
      .option("header", "true")
      .option("inferschema", "true")
      .csv("data/people1.csv")
    dataFrame2.printSchema()
    dataFrame2.show()

    // csv
    val schema1 = "name string,age int,job string"
    val dataFrame3: DataFrame = spark.read.options(Map(("delimiter", ","), ("header", "true")))
      .schema(schema1)
      .csv("data/people1.csv")
    dataFrame3.printSchema()
    dataFrame3.show()

    dataFrame3.write.format("csv")
      .mode("overwrite")
      .save("data/csv")

    // json
    val df3: DataFrame = spark.read.format("json").load("data/emp.json")
    // 不截断
    df3.show(false)
    df3.write.format("json")
      .mode("overwrite")
      .save("data/json")

    // parquet/orc
    val df4: DataFrame = spark.read.format("parquet").load("data/users.parquet")
    df4.show()
    df4.write.format("parquet")
      .mode("overwrite")
      .option("compression","snappy")
      .save("data/parquet")

    // jdbc
    val jdbcDf: DataFrame = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://linux123:3306")
      .option("user", "hive")
      .option("password", "123456")
      .option("dbtable", "")
      .option("numPartitions", 20)
      .load()

    jdbcDf.write.format("jdbc")
      .option("driver","com.mysql.jdbc.Driver")
      .option("url","jdbc:mysql://linux121:3306/?")
      .option("user","hive")
      .option("password","123456")
      .option("dbtable","")
      .mode("append")
      .save()

    spark.close()
  }
}
