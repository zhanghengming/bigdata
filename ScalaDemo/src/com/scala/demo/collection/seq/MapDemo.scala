package com.scala.demo.collection.seq

import scala.collection.mutable

object MapDemo {
  def main(args: Array[String]): Unit = {
    // 定义map 两种方式
    val b = Map("a" -> 1,"b" -> 2)
    val a = Map(('a',1),('b',2),("c",3))
    val keys: Iterable[Any] = a.keys
    val values: Iterable[Int] = a.values
    values.foreach(println(_))
    keys.foreach(println)
    // 访问不存在的key时，会报错
    a("c")//获取map键对应的值
    // 使用get方法，返回一个Option对象，要么是Some（键对应的值），要么是None
    println(a.get('a'))
    // 返回键对应的值，如果不存在则返回给定的值
    a.getOrElse("c",0)

    // 更新map中的值，使用可变的Map
    val c = scala.collection.mutable.Map(("a",1),("b",2))
    // 修改a对应的value
    c("a") = 2
    // 增加一个元素
    c("c") = 3
    // 用 + 添加新的元素；用 – 删除元素
    c += (("e",1),("f",2))
    c -= "a"
    // 遍历map
    for ( a <- c) println(a)
    // k v 互换
    val intToString: mutable.Map[Int, String] = c.map(x => (x._2, x._1))
    println(intToString)
    for (x <- intToString) println(x)

    // 拉链操作
    val arr1 = Array(1,2,3)
    val arr2 = Array("a","b","c")
    val tuples = arr1.zip(arr2).toMap
    println(tuples)
  }
}
