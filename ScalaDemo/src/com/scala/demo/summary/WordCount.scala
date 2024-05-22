package com.scala.demo.summary

import scala.collection.mutable

object WordCount {
  def main(args: Array[String]): Unit = {
    val lines: Array[String] = scala.io.Source.fromFile("src\\work.txt").getLines().toArray
    val array: Array[String] = lines.flatMap(_.split(","))
    /**
     * groupBy返回以入参为分组的值，集合的每个元素组成的集合为值的 kv对
     */
    // 以x进行groupBy，返回以x为键的值为x组成的集合
    val map: Map[String, Array[String]] = array.groupBy(x => x)
    // 和下面等价
    map.map(x => (x._1,x._2.length)).toArray.sortBy(_._2).reverse.foreach(println)
    // 集合中元素的个数就是计数值
    map.mapValues(_.length).toArray.sortBy(_._2).reverse.foreach(println)

    // groupBy效率低
    /**
     * foldLeft:有俩参数，第一个是初始值，也是最后函数返回的类型，第二个参数是一个函数，该函数的入参为一个返回的类型参数和一个集合的元素
     * 遍历集合的每个元素，将第一个参数的值不断迭代然后传入第二个参数函数中的第一个入参，对该参数与第二个入参发生运算后传给初始值，作为函数的返回值
     * wordCount的实现为：先创建一个可变的map集合，作为第一个参数，然后遍历集合中的每个元素，遍历到的元素作为key，遍历到一次+1，直到遍历结束
     * 返回函数的返回值。
     */
    val result: mutable.Map[String, Int] = array.foldLeft(scala.collection.mutable.Map[String, Int]())((maps, words) => {
      maps += (words -> (maps.getOrElse(words, 0) + 1))
    })
    result.toArray.foreach(println)
  }
}
