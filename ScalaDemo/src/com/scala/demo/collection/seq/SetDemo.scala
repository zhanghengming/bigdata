package com.scala.demo.collection.seq

import scala.collection.mutable

object SetDemo {
  def main(args: Array[String]): Unit = {
    // 默认下 使用的是不可变集合
    val set = Set(7,2,3,4,5,6,4,4)
    // 判断元素是否存在 返回Boolean值
    println(set.exists(_ % 2 == 0))
    // 删除元素，返回一个新set，是按元素索引删除，从1开始
    println(set)
    val set1: Set[Int] = set.drop(1)
    println(set1)

    import scala.collection.mutable.Set
    val mutableSet = Set(4,5,6)

    mutableSet.add(7)
    println(mutableSet)
    // 删除元素，传参是元素
    mutableSet.remove(4)
    println(mutableSet)
    // 使用 += / -= 增加、删除元素
    mutableSet += 5
    mutableSet -= 2

    // 交集&\intersect
    println(Set(1,2,3) & Set(2,3,4))
    println(Set(1,2,3).intersect(Set(2,3,4)) )
    println(Set(1,2,3) intersect Set(2,3,4) )
    // 并集 ++、|、union
    println(Set(1,2,3) union Set(2,3,4))
    println(Set(1,2,3) | Set(2,3,4))
    println(Set(1,2,3) ++ Set(2,3,4))
    // 差集 --、&~、diff 返回:包含本集合中不包含在给定集合中的元素的集合
    println(Set(1,2,3) diff Set(2,3,4))
  }
}
