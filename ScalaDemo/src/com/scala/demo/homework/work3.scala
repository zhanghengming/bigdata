package com.scala.demo.homework

import scala.collection.mutable
import scala.io.Source

object work3 {
  def main(args: Array[String]): Unit = {

    val file = Source.fromFile("src\\work.txt")
    val line: Iterator[String] = file.getLines()
    var list: mutable.ListBuffer[((String, String), String, String) ] = mutable.ListBuffer()
    for (elem <- line) {
      val strings = elem.split(",")
      list.append(((strings.toList(0), strings.toList(1)), strings(2), strings(3)))
    }
    println(list.groupBy(_._1))
    println(list.groupBy(x => x))

  }
}
