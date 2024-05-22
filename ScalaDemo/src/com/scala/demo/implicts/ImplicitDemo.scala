package com.scala.demo.implicts

import scala.language.implicitConversions

class Num{}
class RichNum(num:Num) {
  def rich(): Unit = {
    println("hello")
  }
}

object ImplicitDemo {
  // 定义一个名称为num2RichNum的隐式函数
  implicit def num2RichNum(num:Num): RichNum = {
    new RichNum(num)
  }
  def main(args: Array[String]): Unit = {
    val num = new Num
    // num对象没有rich方法，编译器会查找当前范围内是否有可转换的函数
    // 如果定义了一个类型转化为另一个类型的隐式函数，并且影视转换函数中的参数类型的对象在程序中用到了，就会自动调用
    num.rich()
  }
}
