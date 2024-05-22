package com.scala.demo.homework

/**
 * 每瓶啤酒2元，3个空酒瓶或者5个瓶盖可换1瓶啤酒。100元最多可喝多少瓶啤酒？
 */
object work1 {
  var sum = 0
  def beer(money:Int):Int = {
    money / 2
  }
  def exchange(num:Int):Int = {
    if(num < 1) sum
    else {
      sum += (num / 3).toInt + (num / 5).toInt
      exchange((num / 3).toInt + (num / 5).toInt)
    }

  }
  def main(args: Array[String]): Unit = {
    println(exchange(beer(100)) + beer(100))
  }
}
