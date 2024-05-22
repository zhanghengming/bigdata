package com.scala.demo.extend

class Person1(name:String,age:Int)
class Student1(name:String,age:Int,var studentNo:String) extends Person1(name,age)

object TestClass {
  def main(args:Array[String]):Unit = {
    val person: Person1 = new Student1("zhang",12,"1234")
    var student: Student1 = null
    //
    println(person.getClass == classOf[Student1])//true
    //
    if(person.isInstanceOf[Student1]){
      student = person.asInstanceOf[Student1]
    }
    println(student.getClass == classOf[Person1])//false

    person match {
      case e :Student1 => println("它是Student类型的对象")
      case _ => println("ta啥也不是")
    }
  }
}
