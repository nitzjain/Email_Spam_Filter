/**
 * Created by Manpreet Gandhi on 5/9/2016.
 */

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class wongclass {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("CS 286").setMaster("local");
        SparkContext sc = new SparkContext(conf);

        String path = "C:\\Users\\Manpreet Gandhi\\Desktop\\cs286 project\\train567.csv";

        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(path);

        final HashingTF tf = new HashingTF(1000); //what does the parameter do?

        JavaRDD<LabeledPoint> training = df.javaRDD().map(
                new Function<Row, LabeledPoint>() {
                    public LabeledPoint call(Row row) {
                        List<String> list = new ArrayList<String>();
//                        double click;
//                        System.out.println("mapr123 " + row.    size());
                        for (int i = 0; i < row.size(); i++) {
                            if (i == 1) // skip click column
                            {
//                                System.out.println("mapr123 inside loop" + row.get(i));
                                continue;

                            }
                            list.add(row.get(i).toString());
                        }
//                        System.out.println("mapr123 " + row.getDouble(1));
                        LabeledPoint lp = new LabeledPoint(row.getInt(1), tf.transform(list));
                        return lp;
                    }
                }
        );

        //try two different kinds of model
        final LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
                .setNumClasses(2) //what does the parameter do?
                .run(training.rdd()),
                model2 = new LogisticRegressionWithSGD().run(training.rdd());

        model2.save(sc, "C:\\Users\\Manpreet Gandhi\\Desktop\\cs286 project\\");
        Vector test = tf.transform(Arrays.asList("16966404863124028385,14102100,1005,0,1fbe01fe,f3845767,28905ebd,ecad2386,7801e8d9,07d7df22,a99f214a,775fb78b,c6263d8a,1,0,15702,320,50,1722,0,35,-1,79".split(","))),
                test1 = tf.transform(Arrays.asList("16967363799087155610,14102100,1005,0,85f751fd,c4e18dd6,50e219e0,c823f3cc,2347f47a,0f2161f8,a3cada2c,082a7599,d57111ae,1,2,20361,300,250,2333,0,39,-1,157".split(",")));
        System.out.println("Result = " + model2.predict(test) + "\n\t" + model2.predict(test1));

    }
}
