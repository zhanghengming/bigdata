package com.scala.demo.matchCase

class Amount
case class Dollar(value:Double) extends Amount
case class Currency(value: Double, unit: String) extends Amount
case object Nothing extends Amount
// object中只有无参构造器
case object TestObject extends Amount{
  println("case object 是单例的")
}

object CaseClassDemo {
  def main(args: Array[String]): Unit = {
    // 调用函数，传样例类的对象，不用new
    judgeIdentity(Dollar(1.2))
    judgeIdentity(Dollar(1.3))
    judgeIdentity(Currency(1,"123"))
    judgeIdentity(Nothing)
    judgeIdentity(TestObject)
    // case object是单例的，只会创建一个对象，所以只会执行一遍其中的方法
    judgeIdentity(TestObject)

  }
  //自定义方法，模式匹配判断amt类型
  def judgeIdentity(a : Amount): Unit ={
    a match {
      // case 是从上往下执行的，当第一行是_时，就不会匹配下面了
//      case _ => println("...")
      case Dollar(value) => println(value)
      case Currency(value,unit) => println(value,unit)
      case Nothing => println("nothing")
      case TestObject => println("case obj")
    }
  }
}
