package com.scala.demo.implicts

object Doubly {
  //在print函数中定义一个隐式参数fmt
  def print(num:Double)(implicit fmt:String):Unit = {
    println(fmt.format(num))
  }

  def main(args: Array[String]): Unit = {
    //在此之前还没有定义隐式参数，所以需要手动赋值
    print(3.12)(("%.1f"))

    // 定义一个隐式变量
    implicit val printFmt: String = "%.3f"
    //因为上面定义了隐式参数，所以就不用主动赋值
    print(3.12)

  }
}
