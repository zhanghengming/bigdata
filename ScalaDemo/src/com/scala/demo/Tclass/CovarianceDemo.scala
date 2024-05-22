package com.scala.demo.Tclass

/**
 * 协变：只有大师以及大师级别以下的名片都可以进入会场
 */
class Master
class Professor extends  Master
class Teacher

class Card[-T]

object CovarianceDemo {
  // 逆变，professor是master的子类，所以Card[Professor]是Card[Master]的父类，那么就可以传Professor以及他的子类
  def enterMeet(card: Card[Professor]) = {
    println("欢迎入会")
  }
  def main(args: Array[String]): Unit = {
    enterMeet(new Card[Master])
    enterMeet(new Card[Professor])
//    enterMeet(new Card[Teacher])
  }
}
