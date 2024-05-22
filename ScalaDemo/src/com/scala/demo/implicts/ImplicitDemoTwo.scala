package com.scala.demo.implicts

import scala.language.implicitConversions


/**
 * 特殊售票窗口（只接受特殊人群买票，比如学生、老人等），其他人不能在特殊售票窗口买票。
 */

class Student(var name:String)
class Older(var name:String)
class SpecialPerson(var name:String)
class Other(var name:String)

object ImplicitDemoTwo {

  def buySpecialTickWindow(person: SpecialPerson):Unit = {
    if (person != null){
      println(person.name + "可以买特殊票")
    } else {
      println(person.name + "不可以买")
    }
  }

  implicit def person2SpecialPerson(person:Any):SpecialPerson = {
    person match {
      case person:Student => new SpecialPerson(person.name)
      case person:Older => new SpecialPerson(person.name)
      case _ => null
    }
  }

  def main(args: Array[String]): Unit = {
    val student = new Student("zhang")
    val older = new Older("lao")
    val other = new Other("worker")
    // 该方法中调用的是SpecialPerson对象，没有该对象，但是创建了隐式函数，可以将传入的对象转为该对象
    buySpecialTickWindow(student)
    buySpecialTickWindow(older)
    buySpecialTickWindow(other)
  }
}
