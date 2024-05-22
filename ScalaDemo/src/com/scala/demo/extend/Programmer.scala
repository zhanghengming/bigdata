package com.scala.demo.extend

class Programmer (name:String,age:Int){
  def coding(): Unit ={
    println("coding...")
  }
}
class Leader(name:String,age:Int,workNo:String) extends Programmer(name,age){
  override def coding(): Unit ={
    //super.coding()
    println("coding scala")
  }
}

object ExtendsDemo {
  def main(args: Array[String]): Unit = {
    val leader = new Leader("zhang", 12, "100")
    leader.coding()
  }
}