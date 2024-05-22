package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 数据格式：timestamp province city userid adid 时间点 省份 城市 用户 广告
 * 需求： 1、统计每一个省份点击TOP3的广告ID
 * 1562085629599	Hebei	Shijiazhuang	564	1
 * 1562085629621	Hunan	Changsha	14	6
 */
object AdClick {
  def getHour(timelong: String): String = {
    import org.joda.time.DateTime
    val time: DateTime = new DateTime(timelong.toLong)
    time.getHourOfDay.toString
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")
    val N = 3
    val line: RDD[String] = sc.textFile("file:///D:\\新大数据\\6-2（spark）\\spark实战应用（上）\\代码\\data\\advert.log")
    val rawRDD: RDD[Array[String]] = line.map(_.split("\\s+"))
    val value: RDD[((String, String), Int)] = rawRDD.map(raw => ((raw(1), raw(4)), 1)).reduceByKey(_ + _)
    value.map { case ((province, ad), cnt) => (province, (ad, cnt)) }.groupByKey().mapValues(_.toList.sortWith(_._2 > _._2).take(N)).foreach(println)

    // 需求2：统计每个省份每小时 TOP3 的广告ID
    // 根据要求 转换成pairRDD
    rawRDD.map(raw => ((getHour(raw(0)), raw(1), raw(4)), 1)).reduceByKey(_ + _)
      .map(key => ((key._1._1,key._1._2),(key._1._3,key._2)))
      .groupByKey()
      .mapValues(_.toList.sortWith(_._2 > _._2).take(N))
      .foreach(println)

    sc.stop()

    // 每次触发action算子的时候都会往前，因为transformation算子 都是lazy的，其实刚开始并不存在，所以得从刚开始执行



  }
}
