package com.scala.demo.extend

class Person(name:String,age:Int) {
  println("这是父类")
}

class Student(name:String,age:Int,var studentNo:String) extends Person(name, age){
  println("这是子类")
  println(name,age,studentNo)
}
object Demo {
  def main(args: Array[String]): Unit = {
    val student = new Student("zhang", 12, "123")
  }
}
