package com.scala.demo.Tclass

class Student[T1,T2,T3](name: T1) {
  var age:T2 = _
  var address:T3 = _

  def getInfo() = {
    println(name, age, address)
  }

}

object GenericDemo {
  def main(args: Array[String]): Unit = {
    val zhang = new Student[String, Int, String]("zhang")
    zhang.age = 12
    zhang.address = "bj"
    zhang.getInfo()
  }
}
