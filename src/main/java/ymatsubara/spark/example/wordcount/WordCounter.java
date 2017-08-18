package ymatsubara.spark.example.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCounter {
    public static final String APP_NAME = "WordCounter";
    public static final String MASTER = "local[*]";
    public static final String INPUT_FILE_PATH = "src/main/resources/input/wordcount/article*.txt";
    public static final String OUTPUT_DIR_PATH = "src/main/resources/output/wordcount/";

    public static JavaPairRDD<String, Integer> count(JavaRDD<String> textFile) {
        JavaPairRDD<String, Integer> wordCountPair = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word.replace(".", "").toLowerCase(), 1))
                .reduceByKey((a, b) -> a + b);
        return wordCountPair;
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(APP_NAME)
                .setMaster(MASTER);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = sc.textFile(INPUT_FILE_PATH);
        JavaPairRDD<String, Integer> wordCountPair = count(textFile);
        wordCountPair.saveAsTextFile(OUTPUT_DIR_PATH);
    }
}
