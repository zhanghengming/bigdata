package com.scala.demo.traitTest

import scala.util.Sorting

object OrderingDemo {
  def main(args: Array[String]): Unit = {
    val pair = Array(("a", 7, 2), ("c", 9, 1), ("b", 8, 3))
    // Ordering.by[(Int,Int,Double),Int](_._2)表示从Tuple3转到Int型
    // 并按此Tuple3中第二个元素进行排序
    Sorting.quickSort(pair)(Ordering.by[(String,Int,Int),Int](_._2))
    println(pair.toBuffer)
  }
}
