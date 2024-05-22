package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
     val lines: RDD[String] = sc.textFile("hdfs://linux121:9000/wcinput/wc.txt")
//    val lines: RDD[String] = sc.textFile("/wcinput/wc.txt")
    // val lines: RDD[String] = sc.textFile("data/wc.dat")
//    lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).collect().foreach(println)
    // \\s+表示表示匹配空白字符
    val value: RDD[(String, Int)] = lines.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_ + _)
    value.foreach(println)
    sc.stop()
  }
}
