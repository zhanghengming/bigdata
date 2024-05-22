package com.scala.demo.traitTest

trait HelloTrait {
  def sayHello(): Unit
}

trait MakeFriendTrait {
  def makeFriend(): Unit
}

class Person(name:String) extends HelloTrait with MakeFriendTrait with Serializable {
  override def sayHello(): Unit = {
    println("hello:" + name)
  }

  def makeFriend(): Unit ={
      println("name:"+name)
  }
}
object TestTrait {
  def main(args: Array[String]): Unit = {
    val person: Person = new Person("zhang")
    person.sayHello()
    person.makeFriend()
  }
}