package com.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object DS {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("DS").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("warn")
    import spark.implicits._

    /**
     * rdd转DS，Dataset = RDD[case class]
     */
    val arr2 = Array(("Jack", 28, 184), ("Tom", 10, 144), ("Andy", 16, 165))
    val rdd2: RDD[Person] = spark.sparkContext.makeRDD(arr2).map(f => Person(f._1, f._2, f._3))
    val ds: Dataset[Person] = spark.createDataset(rdd2)

    /**
     * 集合生成DS
     */
    val seq1 = Seq(Person("zhang",28,123),Person("shan",25,180))
    val ds1: Dataset[Person] = spark.createDataset(seq1)
    ds1.printSchema()
    ds1.show()

    val seq2 = Seq(("Jack", 28, 184), ("Tom", 10, 144), ("Andy", 16, 165))
    val ds2: Dataset[(String, Int, Int)] = spark.createDataset(seq2)
    ds2.printSchema()
    ds2.show()

    ds.show()
    spark.close()
  }
}
