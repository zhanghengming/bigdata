class Session {
  def hello(first : Int): Int = {
    first
  }
}
object SessionFactory {
  println("这是单例对象的代码！")

  val session = new Session

  def getSession(): Session = {
    session
  }


  def main(args: Array[String]): Unit = {
    val factory = SessionFactory
    // 主构造代码块只能执行一次，因为它是单例的
    val factory1 = SessionFactory

    for (x <- 1 to 10) {
      //通过直接调用，产生的对象都是单例的
      val session1 = SessionFactory.getSession()
      println(session1.hello(x))
      println(session1)
    }
  }
}
