/**
 * Created by Manpreet Gandhi on 5/9/2016.
 */

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import java.util.Arrays;
import java.util.Iterator;


public class adModel {

    public static void main(String[] args)
    {

        SparkConf conf = new SparkConf().setAppName("adModel").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String path = "C:\\Users\\Manpreet Gandhi\\Desktop\\cs286 project\\train567.csv";
        JavaRDD<String> inputRdd = sc.textFile(path);

        Function2 removeHeader= new Function2<Integer, Iterator<String>, Iterator<String>>(){
            @Override
            public Iterator<String> call(Integer ind, Iterator<String> iterator) throws Exception {
                if(ind==0 && iterator.hasNext()){
                    iterator.next();
                    return iterator;
                }else
                    return iterator;
            }
        };

        JavaRDD<String> readFileRDD = inputRdd.mapPartitionsWithIndex(removeHeader, false);

        final HashingTF tf = new HashingTF(1000);

        JavaRDD<LabeledPoint> trainingData = readFileRDD.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String record) {
                String[] tokens = StringUtils.split(record, ","); // 24 tokens

                String[] features = new String[tokens.length-1];

                for (int i = 2; i < tokens.length; i++) {
                    features[i-2] = tokens[i];
                }

                String outcomeClass = tokens[1]; // 1=click, 0=non-click
                return new LabeledPoint(Double.parseDouble(outcomeClass), tf.transform(Arrays.asList(features)));
            }
        });

        // Cache data since Logistic Regression is an iterative algorithm.
        trainingData.cache();

        // Create a Logistic Regression learner which uses the LBFGS optimizer.
        LogisticRegressionWithSGD learner = new LogisticRegressionWithSGD();

        // Run the actual learning algorithm on the training data.
        LogisticRegressionModel model = learner.run(trainingData.rdd());

        model.save(sc.sc(), "ctr_model");

    }
}
