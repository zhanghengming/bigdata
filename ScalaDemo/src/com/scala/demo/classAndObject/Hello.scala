package com.scala.demo.classAndObject

object Hello extends App {
  if (args.length > 0)
    println(s"Hello World; args.length = ${args.length}")
  else
    println("Hello World")
}
