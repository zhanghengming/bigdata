package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FindFriends {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName(this.getClass.getCanonicalName).setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val lines: RDD[String] = sc.textFile("file:///D:\\新大数据\\6-2（spark）\\spark实战应用（上）\\代码\\data\\fields.dat")
    val friendsRDD: RDD[(String, Array[String])] = lines.map(line => {
      val friendArr = line.split(",")
      ((friendArr(0), friendArr(1).trim.split("\\s+")))
    }
    )
    // 笛卡尔积，过滤掉不需要的，然后好友的集合取交集
    friendsRDD.
      cartesian(friendsRDD)
      .filter(friend => friend._1._1 < friend._2._1)
      .map({case (user1,user2) => ((user1._1,user2._1) ,user1._2.intersect(user2._2).toBuffer)})
      .sortByKey()
      .foreach(println)
    println("--------------------------------------------")

    // 方法二  好友是互相的，好友列表中的每个人的好友也都有用户本人，两两组合作为key，然后将相同key的合并起来
    // combinations  求一个集合中任意n个元素的排列组合
    friendsRDD.flatMapValues(x => x.combinations(2))
      .map{case (userId,friendArr) => (friendArr.mkString("&"),Set(userId))}
      .reduceByKey(_ | _)
      .sortByKey()
      .foreach(println)
    sc.stop()


  }
}
