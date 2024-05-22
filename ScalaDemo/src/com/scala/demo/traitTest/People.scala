package com.scala.demo.traitTest

trait People {
  val name:String
  val age = 30
  def eat(message:String): Unit ={
    println(message)
  }
}

