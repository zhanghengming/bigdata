package com.scala.demo.matchCase

import scala.util.Random

object MatchObject {
  def main(args: Array[String]): Unit = {
    val charStr = '6'
    charStr match {
      case '+' => println("匹配上了加号")
      case '-' => println("匹配上了减号")
      case '*' => println("匹配上了乘号")
      case '/' => println("匹配上了除号")
      case  _ => println("都没有匹配上，我是默认值")
    }
    // 字符和字符串匹配
    val arr = Array("hadoop", "zookeeper", "spark")
    val name = arr(Random.nextInt(arr.length))
    name match {
      case "hadoop" => println("hadoop")
      case "spark" =>println("spark")
      case "zookeeper" => println("zk")
      case _ =>println("...")
    }

    // 守卫式匹配
    val character = '*'
    val num = character match {
      case '+' => 1
      case '-' => 2
      //注意：不满足以上所有情况，就执行下面的代码
      case _ if (character=='1') => 3
      case _ => 4
    }
    println(character + " " + num)

    // 匹配类型
    val a = 0
    val obj = if(a == 1) 1
    else if (a == 2) "2"
    else if (a == 3) BigInt(3)
    else if (a == 4) Map("a" -> 1)
    else if (a == 5) Array(1,2,3)
    else if (a == 6) Array("aa",1)
    else if (a == 7) Array("aa")

    val r1 = obj match {
      case x: Int => x
      case s: String => s
      case b: BigInt => b
      case m: Map[String,Int] => m
      case a: Array[Int] => a
      case a: Array[String] => a
      case a: Array[_] => "It's an array of something other than Int"
      case _ => 0
    }
    println(r1 + "," + r1.getClass.getName)
    // 匹配数组、元组、集合
    val str = Array(0,3,5)
    str match {
      //带有指定个数元素的数组、带有指定元素的数组、以某元素开头的数组
      case Array(3,x,y) => println(x + " " + y)
      case Array(0) => println("only 0")
      //匹配数组以1开始作为第一个元素
      case Array(0,_*) => println("1 ...")
      case _ => println("something else")
    }
  }
}
