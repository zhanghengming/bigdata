package com.scala.demo.classAndObject

object ObjectDemo {
  println("单例对象地代码！")

  def hello(): Unit = {
    println("hello")
  }

  def main(args: Array[String]): Unit = {
    val object1 = ObjectDemo
    val object2 = ObjectDemo
    ObjectDemo.hello()
    ObjectDemo.hello()
  }
}
