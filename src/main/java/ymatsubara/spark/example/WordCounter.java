package ymatsubara.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCounter {
    public static void main(String[] args) {
        String master = "local[*]";
        SparkConf conf = new SparkConf()
                .setAppName("WordCounter")
                .setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = sc.textFile("src/main/resources/input/wordcount/article*.txt");
        JavaPairRDD<String, Integer> wordCountPair = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word.replace(".", "").toLowerCase(), 1))
                .reduceByKey((a, b) -> a + b);
        wordCountPair.saveAsTextFile("src/main/resources/output/wordcount/");
    }
}
