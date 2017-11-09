/**
 * Created by Manpreet Gandhi on 5/2/2016.
 */

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class wordcountManpreet {
    public static void main(String[] args) throws Exception {

        // textFile("/my/directory"), textFile("/my/directory/*.txt")
        //textFile("/my/directory/*.gz")
        //JavaSparkContext.wholeTextFiles lets you read a directory containing multiple small text files
        // and returns each of them as (filename, content) pairs

//      SparkConf conf = new SparkConf().setAppName("word count Manpreet").setMaster("local");
        SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local").set("spark.executor.memory","1g");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("C:\\Users\\Manpreet Gandhi\\Desktop\\294 - job\\testdocument.txt");

        JavaRDD<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterable<String> call(String s) {
                        return Arrays.asList(s.split(" "));
                    }
                });

        //transform the collection of words into pairs (word and 1)
        JavaPairRDD<String, Integer> counts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2(s, 1);
                    }
                });

        //count the words
        JavaPairRDD<String, Integer> redCount = counts.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer x, Integer y) {
                        return x + y;
                    }
                });

        redCount.saveAsTextFile("C:\\Users\\Manpreet Gandhi\\Desktop\\294 - job\\output14.txt");

        System.out.println("redCount "+ redCount);
    }
}