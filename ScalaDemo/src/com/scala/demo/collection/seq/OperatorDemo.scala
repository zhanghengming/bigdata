package com.scala.demo.collection.seq
import java.io
import scala.collection.immutable
import scala.collection.mutable.ArrayBuffer

object OperatorDemo {
  def main(args: Array[String]): Unit = {
    val numList = (1 to 10).toList
    /**
     * 两者都用来遍历集合
     * foreach：遍历集合，返回void
     * map：遍历集合的每一项，将集合中的元素做改变后组成新的集合输出
     * mapValues：用于遍历map的values
     */
    numList.foreach(print)
    numList.map(_ * 2).foreach(print)
    println()
    // 返回Boolean集合
    numList.map(_ > 2).foreach(print)
    println("--------------------------------")
    // Range 传参为开始值和结束值（不包含），步长
    // 将集合中的元素转为元组的形式，用索引下标填充
    val map = Range(20,0,-2).zipWithIndex.toMap
    // 将map集合的value值+100
    map.map(elem => (elem._1,elem._2 + 100)).foreach(println)
    map.map { case (k, v) => (k, v + 100) }
    map.mapValues(_ + 100)

    /**
     * flatten：就是将集合中的元素展平，放在一个新集合中
     * flatMap：参数为一个函数，该函数对集合中的每个元素执行一遍map方法，然后将返回的集合展开
     * 可以先flat然后map或者先map后flat
     */
    val list1 = List(List(1,2),List(3,4))
    val flatten: List[Int] = list1.flatten
    println(flatten)
    val arr1 = Array(Some(List(1,2)),None,Some(1,2),None)
    // 数组不能直接输出，输出的是引用
    // flatten 可以将Some和None组成的集合中，将None过滤掉，将Some中的元素组成一个新集合
    val flatten1 = arr1.flatten
    println(flatten1.toBuffer)
    // 将list1中的每个元素*2，然后作为一个集合返回，以下代码等价
    // 先flatten，将集合中的元素展开成一个新集合，然后map 对集合中的每个元素*2
    println(list1.flatten.map(_ * 2))
    // 对集合中的元素（集合）用map方法
    list1.flatMap((x:List[Int]) => x.map(_ * 2))
    list1.flatMap(_.map(_ * 2))

    val lines = Array("Apache Spark has an advanced DAG execution engine",
    "Spark offers over 80 high-level operators")
    // 此时 flatMap = map + flatten
    println(lines.map(_.split(" ")).flatten.toBuffer)
    println(lines.flatMap(_.split(" ")))

    /**
     * collect 通过执行一个偏函数，得到新的数组对象
     * 可以用在对集合中某些值进行特殊处理
     */
    //通过下面的偏函数，把chars数组的小写a转换为大写的A
    // 自定义一个偏函数
    def fun(x:Char): PartialFunction[Char,Char] = {
      case x if x.isLower => x.toUpper
      case x => x
    }
    // 实例化一个偏函数对象
    val pf: PartialFunction[Char, Char] = new PartialFunction[Char, Char] {
      // 满足条件的执行下面的apply方法
      override def isDefinedAt(x: Char): Boolean = {
        if (x.isLower)
          true
          else
          true
      }

      override def apply(v1: Char): Char = {
        v1.toUpper
      }
    }
    val chars = Array('a','b','v','X')
    println(chars.collect(pf).toBuffer)

    var charArray = new Array[Char](chars.length)
    for (elem <- chars) {
      charArray = chars.collect(fun(elem))
    }
    println(charArray.toBuffer)

    /**
     * reduce：对集合中的元素进行规约操作，即将元素反复结合起来，得到一个值
     * reduceLeft 从左向右归约，reduceRight 从右向左归约；
     */
    val lst1 = (1 to 10).toList
    println(lst1.reduce(_ + _))
    // 求最大值
    println(lst1.reduce((x, y) => if (x > y) x else y))
    // reduceLeft 、reduceRight
    println(lst1.reduceLeft((x, y) => if (x > y) x else y))
    println(lst1.reduceRight((x, y) => if (x > y) x else y))

    /**
     * sorted & sortWith & sortBy
     */
    val lst2 = List(1, 9, 3, 8, 5, 6)
    val lst3: List[(String, Int)] = List(("a",1),("c",5),("b",2),("z",3))
    val sorted: List[Int] = lst2.sorted
    println(sorted)
    println(lst2.sortBy(x => x).reverse)
    println(lst2.sortWith(_ > _))
    println(lst3.sortWith(_._1 > _._1))
    //根据集合中的元素，选在哪个进行排序
    println(lst3.sortBy(x => x._2))
  }
}
