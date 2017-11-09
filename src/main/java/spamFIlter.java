/**
 * Created by Manpreet Gandhi on 5/8/2016.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import java.util.Arrays;

public class spamFIlter {

    public static void main(String[] args)
    {

        SparkConf conf = new SparkConf().setAppName("NaiveBayes").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> spam = sc.textFile("files/spam.txt");
        JavaRDD<String> ham = sc.textFile("files/ham.txt");

        // Create a HashingTF instance to map email text to vectors of 100 features.
        final HashingTF tf = new HashingTF(100);

        JavaRDD<LabeledPoint> positiveExamples = spam.map(new Function<String, LabeledPoint>() {
            @Override public LabeledPoint call(String email) {
                return new LabeledPoint(1, tf.transform(Arrays.asList(email.split(" "))));
            }
        });

        JavaRDD<LabeledPoint> negativeExamples = ham.map(new Function<String, LabeledPoint>() {
            @Override public LabeledPoint call(String email) {
                return new LabeledPoint(0, tf.transform(Arrays.asList(email.split(" "))));
            }
        });

        JavaRDD<LabeledPoint> trainingData = positiveExamples.union(negativeExamples);

        JavaRDD<LabeledPoint>[] tmp = trainingData.randomSplit(new double[]{0.6, 0.4}, 12345);

        JavaRDD<LabeledPoint> training = tmp[0]; // training set
        JavaRDD<LabeledPoint> test = tmp[1]; // test set

        final NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);

        JavaPairRDD<Double, Double> predictionAndLabel =
                test.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
                    @Override
                    public Tuple2<Double, Double> call(LabeledPoint p) {
                        return new Tuple2<>(model.predict(p.features()), p.label());
                    }
                });

        double accuracy = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Double, Double> pl) {
                return pl._1().equals(pl._2());
            }
        }).count() / (double) test.count();

        model.save(jsc.sc(), "C:\\Users\\Manpreet Gandhi\\Desktop\\294 - job\\myNaiveBayesModel");
        NaiveBayesModel sameModel = NaiveBayesModel.load(jsc.sc(), "C:\\Users\\Manpreet Gandhi\\Desktop\\294 - job\\myNaiveBayesModel");
        // $example off$

        jsc.stop();
        sc.stop();

    }
}
