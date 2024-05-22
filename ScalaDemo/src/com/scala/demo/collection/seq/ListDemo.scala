package com.scala.demo.collection.seq

object ListDemo {
  def main(args: Array[String]): Unit = {
    // Nil表示一个空列表List[Nothing]
    // :: 是右结合的，list是顺序固定，不是有序的
    // 借助 Nil 可将多个元素用操作符 :: 添加到列表头部，常用来初始化列表；
    val list1 = 1 :: 2 :: 5 :: 4 :: 3 :: Nil
    val list2 = "a" :: "b" :: Nil
    // :::用于拼接两个list
    val list3 = list1 ::: list2
    println(list3)
    println(list3.head)//返回第一个元素
    println(list3.tail)//返回除第一个元素外的列表
    println(list3.last)//返回最后一个元素
    println(list3.init)//返回除最后一个元素外的列表
    println(quickSort(list1))
  }
  // 列表递归的结构，便于编写递归的算法：
  def quickSort(list:List[Int]):List[Int] = {
    list match {
      case Nil => Nil
      case head :: tail =>
        // partition可以将集合分为两部分，满足条件的在前，不满足的在后
        val (less,greater): (List[Int], List[Int]) = tail.partition(_ < head)
        // 将小于头的元素列表和大于头的元素列表做拼接，然后做递归，在子列表里排序
        quickSort(less) ::: head :: quickSort(greater)
    }
  }
}
