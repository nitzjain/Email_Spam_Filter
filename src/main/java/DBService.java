/**
 * Created by Manpreet Gandhi on 5/11/2016.
 */

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import org.apache.spark.api.java.JavaRDD;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;


public class DBService {


    public DBService(){

    }


    public void save(JavaRDD<adCassandraModel> adRDD) {
        CassandraJavaUtil.javaFunctions(adRDD)
                .writerBuilder("cs286", "ad_data1", mapToRow(adCassandraModel.class))
                .saveToCassandra();
    }
}
