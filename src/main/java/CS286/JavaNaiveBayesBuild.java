package CS286;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by Manpreet Gandhi on 5/23/2016.
 */
public class JavaNaiveBayesBuild {

    private static final String SPAM = "C:\\Users\\Manpreet Gandhi\\Desktop\\Lab2-CS286\\spam";
    private static final String HAM = "C:\\Users\\Manpreet Gandhi\\Desktop\\Lab2-CS286\\easy_ham";

    public static void main(String[] args) {

        final HashingTF tf = new HashingTF(999999);

        SparkConf conf = new SparkConf().setAppName("adModel").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        ArrayList<String> spamSubjects = new ArrayList<>();
        ArrayList<String> hamSubjects = new ArrayList<>();

        spamSubjects = returnSubjects(SPAM, "1");
        hamSubjects = returnSubjects(HAM, "0");

        hamSubjects.addAll(spamSubjects);

        JavaRDD<LabeledPoint> inputData = jsc.parallelize(hamSubjects).map(
                new Function<String, LabeledPoint>() {
                    public LabeledPoint call(String s) {
                        String[] a = s.split(",");
                        Double label = Double.parseDouble(a[0]);
                        String vectorResult = s.substring(s.indexOf(",")+1);
//                        System.out.println(a[0] + " - label subject - " + s.substring(s.indexOf(",")+1));
                        return new LabeledPoint(label, tf.transform(Arrays.asList(vectorResult.split(" "))));
                    }
                });

        JavaRDD<LabeledPoint>[] tmp = inputData.randomSplit(new double[]{0.7, 0.4}, 12345);
        JavaRDD<LabeledPoint> trainData = tmp[0];
        JavaRDD<LabeledPoint> testData  = tmp[1];


        final NaiveBayesModel model = NaiveBayes.train(trainData.rdd(), 100.0);

        JavaPairRDD<Double, Double> predictionAndLabel =
                testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
                    @Override public Tuple2<Double, Double> call(LabeledPoint p) {
                        return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
                    }
                });

        double accuracy = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
            @Override public Boolean call(Tuple2<Double, Double> pl) {
                return pl._1().equals(pl._2());
            }
        }).count() / (double) testData.count();

        System.out.println(accuracy + "%");

        model.save(jsc.sc(), "NaiveBayesModel");
//        training.saveAsTextFile("output.txt");
    }

    public static ArrayList<String> returnSubjects(String Path, String type){
        ArrayList<String> Subjects = new ArrayList<String>();
        BufferedReader br = null;
        ArrayList<String> fileNames = fetchFileName(Path);
        for(String fileN: fileNames) {

            try {
                String sCurrentLine;
                String s;
                br = new BufferedReader(new FileReader(Path+"\\"+fileN));

                while ((sCurrentLine = br.readLine()) != null) {
                    if (sCurrentLine.toString().contains("Subject")) {
                        if (sCurrentLine.toString().contains("RE:")) {
                            s = sCurrentLine.substring(sCurrentLine.indexOf("RE:") + 3);
                        } else {
                            s = sCurrentLine.substring(sCurrentLine.indexOf("Subject:") + 8);
                        }
                        Subjects.add(type +","+  s);
                    }
                    else {
                        continue;
                    }
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            finally {
                try {
                    if (br != null) br.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }
        System.out.println(Path + "\n size  is " +  Subjects.size());
        return Subjects;
    }

    public static ArrayList<String> fetchFileName(String s){
        File[] files = new File(s).listFiles();
        if(files == null)
        {
            System.out.println(s + " does not have any files in it");
        }
        ArrayList<String> results = new ArrayList<String>();
        for (File file : files) {
            if (file.isFile()) {
//                    System.out.println(file.getName());
                results.add(file.getName());
            }
        }
        return results;
    }
}
