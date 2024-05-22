package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.random

/**
 * 用蒙特卡洛法计算圆周率
 * 单位正方形内有个内切圆，假设射击的分布均匀
 * 正方形的面积：4 一共射击的次数：N 落在单位圆中的次数：n
 * 4/pi = N/n
 * 点到圆点的距离小于1时，则落在圆内
 */

object SparkPi {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    val slices = if (args.length > 0) args(0).toInt else 10
    val N = 1000000
    val count = sc.makeRDD(1 to N, slices)
      .map(idx => {
        // random 0-1 的随机数
        val (x, y) = (random, random)
        // 在圆内赋值1
        if (x * x + y * y <= 1) 1 else 0
      }).reduce(_ + _)

    println(4.0 * count / N)

    sc.stop()
  }
}
