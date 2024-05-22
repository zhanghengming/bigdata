import org.apache.flink.api.scala._

object WordCountScalaBatch {
  def main(args: Array[String]): Unit = {
    val input =  "D:\\新大数据\\data\\wc.txt"
    val output = "D:\\新大数据\\data\\outputScala"
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val value: DataSet[String] = environment.readTextFile(input)
    val wordAndOne: AggregateDataSet[(String, Int)] = value.flatMap(_.split("\\s+")).map((_, 1)).groupBy(0).sum(1)
    wordAndOne.writeAsCsv(output,"\n"," ").setParallelism(1)
    environment.execute("scala batch process")
  }
}
