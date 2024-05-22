object HelloWorld {
  def main(args: Array[String]): Unit = {
//    for (i <- 1 until  10;j <- 1 to 5){
//      println(s"i * j = ${i * j}")
//    }

    for (x <- 1 to 10){
      println( x )
    }

    val nums = new Array[Int](10)
    nums(9) = 10
    val array = (1 to 10).toArray
    // yield 可以 将for作为返回一个新的数组
    val ints = for (elem <- array) yield elem * 2
    for (in <- ints) println(in)
    println(array.count(_>3))
    // 返回满足过滤条件的集合
    println(array.filter(_ > 3).mkString(","))
    // 返回一个新数组
    println(array.take(4).toBuffer)
    println(array.takeRight(4).mkString(" - "))

    // 返回(1,2),(2,4) 交叉元组 a1,a2的长度不一致时，截取相同的长度
    val tuples = array.zip(ints)
    println(tuples.mkString(","))

    val array1 = array.filter(_ > 5)
    println(array1.zip(array).mkString(","))

    // 数组排序
    val numss = Array(1, 3, 2, 6, 4, 7, 8, 5)
    println(numss.sortWith(_>_).toBuffer)
    println(numss.sorted.reverse.toBuffer)

    // 多维数组
    val dim = Array.ofDim[Double](3,4)
    for ( i <- 0 to 2; j <- 0 to 3) {
      println(dim(i)(j) + " ")
      if (j == 3) println()
    }

    // 元组
    val a = (1, 1.2, "12", 'a')
    val b = Tuple4 (1, 1.2, "12", 'a')
    println(a == b)
    println(a._1,a._2)
    val (a1, a2, a3, a4), a5 = a
    println((a1, a2, a3, a4), a5)
    // 遍历元组
    for (x <- a.productIterator)
      print(x)
    a.productIterator.foreach(x => println(x))
    println("------------------------------")

    println(1.max(10))
    println(1.min(10))
    var y: Int = 0
    val x: Unit = y = 1
    println( y = 1)

    def add(x:Int*):Int = {
      x.sum
    }

    println(add(1 to 10:_*))
    println("------------------------------")
    import scala.collection.mutable.ArrayBuffer
    val arrays = ArrayBuffer[Int]()
    arrays += 1
    arrays.foreach(println(_))
    // 增强for循环
    for(elm <- arrays){
      println(elm)
    }
    // foreach
    arrays.foreach(println(_))
    val numsss = Array(1, 3, 2, 6, 4, 7, 8, 5)
    numsss.sorted
    println(numsss.toBuffer)


    val a12 = (1,"a",'a',1.2)
    // 访问元组的元素
    // 从元组接收数据  a1到a4代表元组的4个元素，a5代表元组
    val (a111,a222,a333,a444),a555 = a12
    val (b1,_,_,b2) = a12

    for(t <- a12.productIterator){
      println(t)
    }
    a12.productIterator.foreach(x => println(x))

    val change = null
  }


}
