import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
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
        //spark.sql("CREATE TABLE IF NOT EXISTS src (TIMESTAMP long, TIMESTAMP_USEC long,TIMESTAMP_MICROS long) USING hive OPTIONS(fileFormat 'org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat',outputFormat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',serde 'MySerDe')");
        //spark.sql("CREATE TABLE IF NOT EXISTS src (ts bigint,ts_micros bigint,ttl int,ip_version int,ip_header_length int) USING hive OPTIONS(fileFormat 'sequencefile',serde 'PcapDeserializer')");
        spark.sql("CREATE TABLE IF NOT EXISTS src (ts bigint, ts_usec double, protocol string, src string, src_port int, dst string, dst_port int, len int, ttl int, dns_queryid int, dns_flags string, dns_opcode string, dns_rcode string, dns_question string, dns_answer array<string>, dns_authority array<string>, dns_additional array<string>) USING hive OPTIONS(fileFormat 'sequencefile',serde 'PcapDeserializer')");
        spark.sql("LOAD DATA LOCAL INPATH '/home/bjbhaha/Envroment/hadoop-2.7.3/bin/music31.seq' INTO TABLE src");

// Queries are expressed in HiveQL
        spark.sql("SELECT * FROM src where src='10.222.181.148' and dst='120.240.50.212'").show();
        //spark.sql("SELECT * FROM src").show();
    }
}
