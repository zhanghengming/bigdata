package com.scala.demo.collection.seq

import scala.collection.mutable

object Queue {
  def main(args: Array[String]): Unit = {
    // 创建一个可变的队列
    val queue = new mutable.Queue[Int]()
    println(queue)
    // 队列中添加一个元素
    queue += 1
    // 队列中添加list
    queue ++= List(2,3,4,1)
    println(queue)

    // 返回队列中的第一个元素，并删除，先进先出
    val i: Int = queue.dequeue()
    println(queue)
    // 元素入队
    queue.enqueue(5,6,7)
    //获取第一个、最后一个元素
    println(queue.head)
    println(queue.last)

  }
}
