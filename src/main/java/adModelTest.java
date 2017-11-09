/**
 * Created by Manpreet Gandhi on 5/9/2016.
 */

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.commons.lang.StringUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.mllib.feature.HashingTF;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.linalg.Vector;

import java.util.Arrays;
import java.util.Iterator;

public class adModelTest {

    static HashingTF tf = new HashingTF(1000);

    static Tuple2<String, Vector> buildVector(String record) {
        String[] features = new String[22];
        String[] tokens = StringUtils.split(record, ","); // 23 tokens
        String adID = tokens[0];
        for (int i = 1; i < tokens.length; i++) {
            features[i - 1] = tokens[i];
        }
        //
        Vector v = tf.transform(Arrays.asList(features));
        return new Tuple2(adID, v);
    }

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("adModelPrediction")
                .setMaster("local[*]")
                .set("spark.cassandra.connection.host", "127.0.0.1");

        //cassandra
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        Session session = cluster.connect("cs286");

        JavaSparkContext sc = new JavaSparkContext(conf);

        DBService db =  new DBService();



        String path = "C:\\Users\\Manpreet Gandhi\\Desktop\\cs286 project\\test.csv";

        JavaRDD<String> queryRdd = sc.textFile(path);

        Function2 removeHeader = new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer ind, Iterator<String> iterator) throws Exception {
                if (ind == 0 && iterator.hasNext()) {
                    iterator.next();
                    return iterator;
                } else
                    return iterator;
            }
        };

        JavaRDD<String> query = queryRdd.mapPartitionsWithIndex(removeHeader, false);

        JavaPairRDD<String, String> inputRdd = query.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) {
                String[] inputSplit = s.split(",");
                return new Tuple2<String, String>(inputSplit[0], s.substring(1 + s.indexOf(",")));
            }
        });

        // LOAD the MODEL from saved PATH:
        //
        //   public static LogisticRegressionModel load(SparkContext sc, String path)
        final LogisticRegressionModel model = LogisticRegressionModel.load(sc.sc(), "ctr_model");

        JavaPairRDD<String, String> classifications = query.mapToPair(
                new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String, String> call(String record) {
                        // each record has this format:
                        //      <ad-id><,><feature-1><,><feature-2>...<feature-30>
                        Tuple2<String, Vector> pair = buildVector(record);
                        Vector v = pair._2;
                        String adID = pair._1;
                        double classification = model.predict(v);
                        String prediction = new Double(classification).toString();
                        //
                        return new Tuple2<String, String>(adID, prediction);
                    }
                });


        classifications.cache();
        inputRdd.cache();

        JavaPairRDD<String, String> adToBeClicked = classifications.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> s) throws Exception {
                if(s._2().contains("1"))
                {
                     return true;
                }
                return false;
            }
        });

        JavaPairRDD<String, Tuple2<String, String>> joinsOutput = adToBeClicked.join(inputRdd);


        JavaRDD<adCassandraModel> saveRddUdf = joinsOutput.map(new Function<Tuple2<String, Tuple2<String, String>>, adCassandraModel>() {
            @Override
            public adCassandraModel call(Tuple2<String, Tuple2<String, String>> str) throws Exception {
                adCassandraModel aMM = new adCassandraModel();
                aMM.setAdID(str._1());
                aMM.setClick(str._2()._1());
                String s[] = str._2()._2().split(",");
                aMM.setHour(Integer.valueOf(s[0]));
                aMM.setC1(Integer.valueOf(s[1]));
                aMM.setBannerPos(Integer.valueOf(s[2]));
                aMM.setSiteId(s[3]);
                aMM.setSiteDomain(s[4]);
                aMM.setSiteCategory(s[5]);
                aMM.setAppId(s[6]);
                aMM.setAppDomain(s[7]);
                aMM.setAppCategory(s[8]);
                aMM.setDeviceId(s[9]);
                aMM.setDeviceIp(s[10]);
                aMM.setDeviceModel(s[11]);
                aMM.setDeviceType(Integer.valueOf(s[12]));
                aMM.setDeviceConnType(Integer.valueOf(s[13]));
                aMM.setC14(Integer.valueOf(s[14]));
                aMM.setC15(Integer.valueOf(s[15]));
                aMM.setC16(Integer.valueOf(s[16]));
                aMM.setC17(Integer.valueOf(s[17]));
                aMM.setC18(Integer.valueOf(s[18]));
                aMM.setC19(Integer.valueOf(s[19]));
                aMM.setC20(Integer.valueOf(s[20]));
                aMM.setC21(Integer.valueOf(s[21]));

                return aMM;
            }
        });


        saveRddUdf.cache();

        db.save(saveRddUdf);

        session.close();
        cluster.close();
//        joinsOutput.saveAsTextFile("output.csv");
        Iterable<Tuple2<String, String>> predictions = adToBeClicked.collect();

        for (Tuple2<String, String> pair : predictions)
        {
                System.out.println("query: adID=" + pair._1);
                System.out.println("prediction=" + pair._2);
        }

        sc.close();
    }
}
