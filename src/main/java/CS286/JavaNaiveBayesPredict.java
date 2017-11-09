package CS286;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by Manpreet Gandhi on 5/24/2016.
 */
public class JavaNaiveBayesPredict {

    public static void main(String[] args) {
        final HashingTF tf = new HashingTF(999999);

        SparkConf conf = new SparkConf().setAppName("JavaNaiveBayesPredict").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        String Path = "C:\\Users\\Manpreet Gandhi\\Desktop\\Lab2-CS286\\inputSpam";

        NaiveBayesModel sameModel = NaiveBayesModel.load(jsc.sc(), "NaiveBayesModel");

        ArrayList<String> inputData = returnSubjects(Path);

        JavaRDD<LabeledPoint> LabeledData = jsc.parallelize(inputData).map(
                new Function<String, LabeledPoint>() {
                    public LabeledPoint call(String s) {
                        String[] a = s.split(",");
                        Double label = Double.parseDouble(a[0]);
                        String vectorResult = s.substring(s.indexOf(",")+1);
//                        System.out.println(a[0] + " - label subject - " + s.substring(s.indexOf(",")+1));
                        return new LabeledPoint(label, tf.transform(Arrays.asList(vectorResult.split(" "))));
                    }
                });

    }

    public static ArrayList<String> returnSubjects(String Path){
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
                        Subjects.add(s);
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
