package com.spark.core


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement}

object SuperWordCount {

  private val stopWords = "in on to from by a an the is are were was i we you your he his some any of as can it each"

  private val punctuation = "[\\)\\.,:“’;'!\\?]"

  val username = "root"
  val pwd = "123456"
  val url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf-8&useSSL=false"


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val lines: RDD[String] = sc.textFile("file:///D:\\新大数据\\6-2（spark）\\spark实战应用（上）\\代码\\data\\swc.dat")
    val result: RDD[(String, Int)] = lines.flatMap(_.split("\\s+"))
      .map(_.trim.toLowerCase)
      //将特殊字符替换
      .map(_.replaceAll(punctuation, ""))
      // 将停用词过滤掉，看停用词是否包含每个单词，过滤出不包含的
      .filter(word => !stopWords.contains(word) && word.trim.length > 0)
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)

    result.collect.foreach(println)
    //输出到本地文件
//    result.saveAsTextFile("file:///D:\\新大数据\\6-2（spark）\\spark实战应用（上）\\代码\\data\\output")
//输出到MySQL

//    var conn: Connection = null
//    var stmt: PreparedStatement = null
//
//    val sql = "insert into wordcount values(?,?)"



    // 循环将结果输出
/*    result.foreach{case (word,count) =>
      try {
        // 写到外面会报没有序列化异常，因为外面是定义在driver端，执行的时候需要序列化后传到executor，解决方案就是对象的创建放在函数内
        // 这样就会引起另一个问题，就是频繁地创建对象，每循环一遍就得创建连接
        //  使用foreachPartition,每个分区创建一个连接
        conn = DriverManager.getConnection(url,username,pwd)
        stmt = conn.prepareStatement(sql)
        stmt.setString(1, word)
        stmt.setInt(2, count)
        stmt.execute()
      } catch {
        case e:Exception => e.printStackTrace()
      } finally {
        if (stmt != null) stmt.close()
        if (conn != null) conn.close()
      }
    }*/


    // 优化
    result.foreachPartition(
      iterator => {
      var conn:Connection = null
      var preparedStatement:PreparedStatement = null
      val sql = "insert into wordcount values (?, ?)"
      try {
        conn = DriverManager.getConnection(url, username, pwd)
        preparedStatement = conn.prepareStatement(sql)
       iterator.foreach{
        case (word,count) =>
           preparedStatement.setString(1, word)
           preparedStatement.setInt(2, count)
           preparedStatement.execute()
       }
      } catch {
        case e:Exception => e.printStackTrace()
      } finally {
        if (preparedStatement != null) preparedStatement.close()
        if (conn != null) conn.close()
      }
    })

    result.foreachPartition(saveAsMysql)

    sc.stop()
  }

  def saveAsMysql(iterator: Iterator[(String,Int)]): Unit = {
    var conn:Connection = null
    var preparedStatement:PreparedStatement = null
    val sql = "insert into wordcount values (?, ?)"
    try {
      conn = DriverManager.getConnection(url, username, pwd)
      preparedStatement = conn.prepareStatement(sql)
      iterator.foreach{
        case (word,count) =>
          preparedStatement.setString(1, word)
          preparedStatement.setInt(2, count)
          preparedStatement.execute()
      }
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      if (preparedStatement != null) preparedStatement.close()
      if (conn != null) conn.close()
    }
  }
}
