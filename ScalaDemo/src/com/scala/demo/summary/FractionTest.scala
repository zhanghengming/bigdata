package com.scala.demo.summary

import scala.language.implicitConversions

class Fraction(n:Int,m:Int) extends Ordered[Fraction] {
  // 分子和分母的最公约数
  private val commonDivisor = gcd(n,m)
  // 化简后的分子、分母
  private val numerator = n / commonDivisor
  private val denominator = m / commonDivisor

  // 找最大公约数
  def gcd(a:Int,b:Int):Int = {
    if (b == 0) a
    else gcd(b,a%b)
  }

  // 加法
  def +(a:Fraction): Fraction = {
    val x = this.numerator * a.denominator  + a.numerator * this.denominator
    val y = this.denominator * a.denominator
    Fraction(x,y)
  }
  // 分数加整数
  def +(b:Int): Fraction = {
    val x = this.numerator + b * this.denominator
    val y = this.denominator
    Fraction(x,y)
  }
  // 继承ordered重写compare方法
  override def compare(x: Fraction): Int = {
      (this.numerator.toDouble/this.denominator).compare(x.numerator.toDouble/x.denominator)
  }
  override def toString: String = s"$numerator/$denominator"

}
object Fraction {
  def apply(n: Int, m: Int): Fraction = new Fraction(n, m)

  // 接收一个对象，从中提取值，用于模式匹配对象构造函数参数列表
  def unapply(arg: Fraction): Option[(Int, Int)] = Some(arg.numerator, arg.denominator)

  // 将数组中的不同类型的转为同一类型，然后就可以排序了
  implicit def intToFraction(int: Int):Fraction = Fraction(int,int * int)
}

object FractionTest {
  def main(args: Array[String]): Unit = {

    println(Fraction(2, 3) + Fraction(3, 4))
    println(Fraction(2, 3) + 1)
    val array: Array[Fraction] = Array(Fraction(2, 4), Fraction(4, 3), 1)
    array.sorted.foreach(println)
    println("--------------")
    for (elem <- array) {
      elem match {
        case elem: Fraction => println(1)
        case Fraction(2,_) => println(elem,1)
        case _ => println(elem)
      }
    }
  }
}
