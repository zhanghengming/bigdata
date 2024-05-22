package com.scala.demo.function

object FunctionDemo {

  def main(args: Array[String]): Unit = {
    val list = (1 to 10).toList
    def double(x:Int):Int = x * x
    list.map(double(_))
    // 方法转为函数
    def f1 = double _

    println(f1(2))
    list.reduce((x,y) => x + y)

    /**
     * 高阶函数：接收一个或多个函数作为输入 或 输出一个函数
     */
    val func = n => "*" * n
    (1 to 5).map(func(_)).foreach(println)
    // 定义一个函数，两个入参 返回一个函数，这个函数有两个参数
    val urlBuilder = (ssl: Boolean, domain: String) => {
      val schema = if(ssl) "https://" else "http://"
      (endPoint: String, query: String) =>
        s"$schema$domain/$endPoint?$query"
    }
    val domain = "www.baidu.com"
    // 用def定义函数
    def getUrl = urlBuilder(true,domain)
    val endPoint: String = "show"
    val query = "id=1"
    val url = getUrl(endPoint,query)
    println(url)

    println("------------------闭包-----------------")
    /**
     * 闭包：需要引用到函数外面定义的变量的函数
     * 需要满足三个条件：
     * 1.闭包是一个函数
     * 2.函数必须有返回值
     * 3.返回值依赖声明在函数外部的一个或多个变量
     */
    val addMore1 = (x: Int) => x + 10
    var more = 10
    val addMore2 = (x: Int) => {
      x + more
      more = 30
      x + more
    }
    println(addMore1(1))
    more = 20
    println(addMore2(2))

    /**
     * 柯里化
     * 接收多个参数的函数都可以转化为接收单个参数的函数，这个转化过程就是柯里化
     * 和普通函数相比，区别在于柯里化函数拥有多组参数列表，每组参数用（）
     */
    def add(x: Int) = (y :Int) => x + y
    def add1(x: Int)(y :Int) = x + y

    println(add(1))
    println(add1(1)(2))

    /**
     * 部分应用函数：缺少部分参数的函数
     * 如果一个函数有n个参数, 而为其提供少于n个参数, 那就得到了一个部分应用函数。
     */
    def adds(x: Int, y: Int, z:Int) = x + y + z
    def addx = adds(1,_: Int,_: Int)
    // Int不能省略
    def addxAndy = adds(10, 100, _:Int)

    println(addxAndy(1))
    println(addx(2, 3))
    // 省略了全部参数 下面两个等价
    def adda = adds(_: Int,_: Int,_: Int)
    def addb = adds _

    /**
     * 偏函数（Partial Function）：并不处理所有输入的参数，只处理能匹配到case语句的参数
     * 整个函数用{}包围，被{}包住的一组case就是一个偏函数
     * Scala中的偏函数是一个trait，类型为PartialFunction[A,B]，表示：接收一个类型为A的参数，返回一个类型为B的结果。
     */
    // 定义一个偏函数， 接收类型为int的参数，返回string类型的结果
    val pf: PartialFunction[Int, String] = {
      case 1 => "one"
      case 2 => "two"
      case _ => "other"
    }
    println("-----------------------------")
    val list1: List[Any] = List(1, "aa", 2, "hadoop")
    val list2 = list1.drop(2)
    list2.foreach(println)

    // 实例化一个偏函数特质，并重写方法
    val partialFun = new PartialFunction[Any,Int] {
      // 如果返回true，就调用apply构建实例对象，如果返回false，过滤string数据
      override def isDefinedAt(x: Any): Boolean = {
        x.isInstanceOf[Int]
      }

      override def apply(v1: Any): Int = {
        v1.asInstanceOf[Int] + 1
      }
    }
    // collection通过执行一个偏函数，得到一个新的数组对象
    list1.collect(partialFun).foreach(println)
    list1.collect{case x:Int => x+1}.foreach(println)
  }
}
