/**
 * Created by Manpreet Gandhi on 5/5/2016.
 */

import org.apache.spark.SparkConf;
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

public class naiveBayesSpam {

    public static void main(String[] args)
    {
//        SparkConf conf = new SparkConf().setAppName("NaiveBayes").setMaster("local").set("spark.executor.memory","1g");
        SparkConf conf = new SparkConf().setAppName("NaiveBayes").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        String path = "C:\\Users\\Manpreet Gandhi\\Desktop\\294 - job\\sample_naive_bayes.txt";

        JavaRDD<String> readFileRDD=jsc.textFile(path);
//        JavaRDD<LabeledPoint>  labeledPointJavaRDD=readFileRDD.map(new Function<String, LabeledPoint>() {
//            @Override
//            public LabeledPoint call(String s) throws Exception {
//                String lp[]=s.split(",");
//                Double val1=Double.parseDouble(lp[1].split(" "));
//                return new LabeledPoint(Double.valueOf(lp[0]), Vectors.dense(Double.parseDouble(lp[1].split(" ")),)
//            }
//        })
//        JavaRDD<LabeledPoint> inputData = MLUtils.loadLibSVMFile(jsc.sc(), path).toJavaRDD();
        JavaRDD<LabeledPoint> inputData = MLUtils.loadLibSVMFile(jsc.sc(), path).toJavaRDD();
        System.out.println("************************************");
        System.out.println();
        System.out.println("RDD loaded in labeled point");
        System.out.println();
        System.out.println("************************************");

        JavaRDD<LabeledPoint>[] tmp = inputData.randomSplit(new double[]{0.6, 0.4}, 12345);

        System.out.println("************************************");
        System.out.println();
        System.out.println("Random split is done for test and sample data");
        System.out.println();
        System.out.println("************************************");

        JavaRDD<LabeledPoint> training = tmp[0]; // training set
        JavaRDD<LabeledPoint> test = tmp[1]; // test set

        System.out.println("************************************");
        System.out.println();
        System.out.println("training and test set assigned");
        System.out.println();
        System.out.println("************************************");

        final NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);

        System.out.println("************************************");
        System.out.println();
        System.out.println("train method is called");
        System.out.println();
        System.out.println("************************************");

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

        // Save and load model
        model.save(jsc.sc(), "C:\\Users\\Manpreet Gandhi\\Desktop\\294 - job\\myNaiveBayesModel");
        NaiveBayesModel sameModel = NaiveBayesModel.load(jsc.sc(), "C:\\Users\\Manpreet Gandhi\\Desktop\\294 - job\\myNaiveBayesModel");
        // $example off$

        jsc.stop();


    }

}
