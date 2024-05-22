package com.scala.demo.matchCase

object OptionMatch {
  val grades: Map[String, Int] = Map("hadoop" -> 20, "scala" -> 30, "spark" -> 80)
  // 获取键对应的值，来匹配有无值
  def judge(name:String): Unit ={
    val key = grades.get(name)
    key match {
      // 如果key有值
      case Some(key) => println(key)
      case None => println("nothing")
    }
  }

  def main(args: Array[String]): Unit = {
    judge("hadoop")
    judge("flink")
  }
}
