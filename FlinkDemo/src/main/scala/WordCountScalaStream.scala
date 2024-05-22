import org.apache.flink.streaming.api.scala._

object WordCountScalaStream {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val StreamSource: DataStream[String] = environment.socketTextStream("linux121", 7777)
    val value: DataStream[(String, Int)] = StreamSource.flatMap(_.split("\\s+")).map((_, 1)).keyBy(0).sum(1)
    value.print()
    environment.execute()
  }
}
