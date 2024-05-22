import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class JavaWordCount {
    public static void main(String[] args) {
        // 1 创建 JavaSparkContext
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        javaSparkContext.setLogLevel("warn");

        // 2 将读取到的文件转为RDD
        /// "file:///C:\\Project\\LagouBigData\\data\\wc.txt" 本地文件前面加file:///
        JavaRDD<String> file = javaSparkContext.textFile("hdfs://linux121:9000/wcinput/wc.txt");
        // 3 对每一行进行切割 参数为lambda表达式
        JavaRDD<String> words = file.flatMap(line -> Arrays.stream(line.split(" ")).iterator());

        // 4 对元素进行转化成kv的元组 转成PairRDD
        JavaPairRDD<String, Integer> wordMap = words.mapToPair(word -> new Tuple2<>(word, 1));

        // 5 lambda表达式
        JavaPairRDD<String, Integer> results = wordMap.reduceByKey((x, y) -> x + y);

        results.foreach(elem -> System.out.println(elem));

        javaSparkContext.stop();
    }
}
