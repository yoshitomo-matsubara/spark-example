package ymatsubara.spark.example.grading;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Grader {
    public static final String APP_NAME = "Grader";
    public static final String MASTER = "local[*]";
    public static final String INPUT_FILE_PATH = "src/main/resources/input/grading/*.txt";
    public static final String OUTPUT_DIR_PATH = "src/main/resources/output/grading/";

    public static Tuple2<String, Integer> extractIdAndScore(String line) {
        String[] elements = line.split("\t");
        return new Tuple2<>(elements[0], Integer.parseInt(elements[1]));
    }

    public static JavaPairRDD<String, Integer> grade(JavaRDD<String> textFile) {
        JavaPairRDD<String, Integer> gradePair = textFile
                .mapToPair(line -> extractIdAndScore(line))
                .reduceByKey((a, b) -> a + b);
        return gradePair;
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(APP_NAME)
                .setMaster(MASTER);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = sc.textFile(INPUT_FILE_PATH);
        JavaPairRDD<String, Integer> gradePair = grade(textFile);
        gradePair.saveAsTextFile(OUTPUT_DIR_PATH);
    }
}
