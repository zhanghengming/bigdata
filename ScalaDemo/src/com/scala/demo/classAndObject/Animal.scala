package com.scala.demo.classAndObject

abstract class Animal {
  def speak(): Unit
}
class Dog extends Animal {
  override def speak():Unit = {
    println("wow")
  }
}
class Cat extends Animal {
  override def speak():Unit = {
    println("miao")
  }
}
object Animal {
  def apply(str:String):Animal = {
    if(str == "dog")
      new Dog
    else
      new Cat
  }
  def main(args:Array[String]): Unit = {
    val dog = Animal("dog")
    dog.speak()
    val cat = Animal("cat")
  }
}
