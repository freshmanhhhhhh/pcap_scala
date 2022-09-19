import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
public class pcaptest2 {
    public static void main(String args[]){
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();
        spark.sql("CREATE TABLE IF NOT EXISTS src (key int, value binary) USING hive OPTIONS(fileFormat 'sequencefile')");
        spark.sql("LOAD DATA LOCAL INPATH '/home/bjbhaha/Envroment/hadoop-2.7.3/bin/music31.seq' INTO TABLE src");

// Queries are expressed in HiveQL
        spark.sql("SELECT * FROM src").show();
    }
}
