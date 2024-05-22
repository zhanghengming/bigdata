package com.scala.demo.classAndObject

class ClassObject {
  val id = 1
  private var name = "zhang"
  def printName(): Unit = {
    println(ClassObject.constant + name)
  }
}
object ClassObject{
  private val constant = "123"

  def main(args: Array[String]): Unit = {
    val p = new ClassObject
    p.name = "456"
    p.printName()
  }
}