package com.scala.demo.homework

import scala.io.StdIn
import scala.util.Random

class Person() {
  var score = 0

  def showFist(str: String,st: String): Int = {
    (str,st) match {
      case ("石头","布") | ("剪刀","石头") | ("布","剪刀") =>  println("你输了"); 0
      case ("石头","石头") | ("剪刀","剪刀") | ("布","布") =>  println("平局"); 1
      case ("石头","剪刀") | ("剪刀","布") | ("布","石头") =>  println("你赢了");2
    }
  }
}
class Computer {
  var score = 0
  def showFist(): String = {
    Random.nextInt(3) match {
      case 0 => "石头"
      case 1 => "剪刀"
      case 2 => "布"
    }
  }
}

object work2 {

  def main(args: Array[String]): Unit = {
    val computer = new Computer
    val person = new Person
    var flag = true
    while (flag) {
      print("请您出拳：")
      val out = StdIn.readLine()
      if (out != "n" ) {
//        if (("石头","剪刀","布"))
        print("电脑出拳：")
        val co = computer.showFist()
        print(co)
        println()
        val i = person.showFist(out, co)
        i match {
          case 0 => computer.score += 2
          case 1 => computer.score += 1
          case 2 => computer.score += 0
        }
        person.score += i
      } else {
        flag = false
      }
      println("-------------------")
    }
    println("游戏结束")
    println("你的得分："+person.score)
    println("计算机得分："+computer.score)

  }
}
