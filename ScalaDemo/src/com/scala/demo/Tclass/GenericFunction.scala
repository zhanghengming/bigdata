package com.scala.demo.Tclass

object GenericFunction {
  def getCard[T](content: T):Unit = {
    content match {
      case content: Int => println("int")
      case content: String => println("String")
      case _ => println("nothing")
    }
  }
  def main(args: Array[String]): Unit = {
    getCard("string")
    getCard[Int](12)
    getCard('c')
    getCard()
    getCard[Unit]()
  }
}
