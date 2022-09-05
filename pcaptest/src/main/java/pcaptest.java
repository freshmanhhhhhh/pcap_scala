import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.file.tfile.ByteArray;
import org.apache.hadoop.mapreduce.InputSplit;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.spark.api.java.JavaNewHadoopRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
//import scala.Tuple2;
//import scala.Tuple3;
import scala.Tuple2;
import scala.Tuple3;
//import scala.collection.Iterator;
//import scala.collection.immutable.List;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.*;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
public class pcaptest {
    public static void toIp(byte[] ip){

    }
    //    public static void printByte(ByteArray ba){
//        for(int i=0;i<ba.size();i++){
//            System.out.print(ba.)
//        }
//    }
    class MaxLength implements Function2<byte[], byte[],byte[]> {
        @Override
        public byte[] call(byte[] bytes, byte[] bytes2) throws Exception {
            if(bytes.length>bytes2.length)  return bytes;
            else return bytes2;
        }
    }

    public static String binary(byte[] bytes, int radix){
        return new BigInteger(1, bytes).toString(radix);// 这里的1代表正数
    }
    public static String byteToStr(byte[] b) {
        return Arrays.toString(b);
    }
    public static String byteArrayToHexStr(byte[] byteArray) {
        if (byteArray == null){
            return null;
        }
        char[] hexArray = "0123456789ABCDEF".toCharArray();
        char[] hexChars = new char[byteArray.length * 2];
        for (int j = 0; j < byteArray.length; j++) {
            int v = byteArray[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }
    public static void main(String args[]){
        SparkConf conf = new SparkConf().setAppName("pcaptest").setMaster("local[12]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<IntWritable, BytesWritable> javaRDD =sc.sequenceFile(args[0], IntWritable.class, BytesWritable.class);
        //javaRDD.cache();
        //JavaPairRDD<IntWritable,Text> javaRDD2=javaRDD.ToPair(v2->v2);
        //javaRDD.foreach(x -> x._2.get());
        //javaRDD.cache();
        // JavaPairRDD<IntWritable, ByteArray> javaRDD2=

//        byte[] a={new byte("cf"),c,d};
//        int b=a.length();
        //List<Byte> lista= new ArrayList<Byte>() {};
        //ByteArray ba=
        //List<BytesWritable> list4=new ArrayList<BytesWritable>();


      //  List<String> aa=new ArrayList<>();

        //int []aaa=new int[1];
        //System.out.println("aa:");javaRDD.foreach(x-> aaa[0]=x._1().get());
        int i=1;
       // System.out.println("a:");javaRDD.foreach(x-> System.out.println(" "+x._1()+" "+x._2().getLength()+" "+x._2()));






//        String path = "/home/bjbhaha/Desktop/music8.seq";
//        try  {
//            JavaNewHadoopRDD<Text, BytesWritable> recordsRdd = (JavaNewHadoopRDD<Text, BytesWritable>) sc
//                    .newAPIHadoopFile(path, SequenceFileInputFormat.class,
//                            Text.class, BytesWritable.class, sc.hadoopConfiguration());
//            //recordsRdd.foreach(x->System.out.println(x._1()+" "+x._2()));
//            JavaRDD<Tuple3<String, String, String>> tuple3JavaRDD = recordsRdd.mapPartitionsWithInputSplit((Function2<InputSplit,
//                    Iterator<Tuple2<Text, BytesWritable>>,
//                    Iterator<Tuple3<String, String, String>>>) (is, dataIterator) -> {
//                String filePath = ((FileSplit) is).getPath().toString();
//                LinkedList<Tuple3<String, String, String>> list = new LinkedList<Tuple3<String, String, String>>();
//                while (dataIterator.hasNext()) {
//                    Tuple2<Text, BytesWritable> data = dataIterator.next();
//                    BytesWritable value = data._2();
//                    value.setCapacity(value.getLength());
//                    String valueContent = new String(value.getBytes(), StandardCharsets.UTF_8);
//                    // 只改了这一行，可以新建个Text对象，或像下面一样(注意Tuple类型改变需改动相关几处代码)
//                    list.add(new Tuple3<>(filePath, data._1().toString(), valueContent));
//                }
//
//                return list.iterator();
//            }, true);
//
//            tuple3JavaRDD.foreach((VoidFunction<Tuple3<String, String, String>>) v -> {
//                // 此时v._2()输出就是正常的每行数据实际值了
//                System.out.println("fileName:{"+v._1()+"},key:{"+v._2()+"},val length:{"+v._3().length()+"}");
//            });
//        } catch (Exception e) {
//            e.printStackTrace();
//        }


        //javaRDD.foreach(x-> aa.add(binary(x._2().getBytes(),16)));

        // List<Tuple2<IntWritable, BytesWritable>> aa1=javaRDD.collect();
        //System.out.println("a:");javaRDD.foreach(x-> aa.add(new ByteArray(x._2())));
        //List<byte[]> list4=new ArrayList<>();
        //System.out.println("a:");javaRDD.foreach(x->System.out.println(x._2.getBytes()));

        /*JavaRDD<String> rdd3=javaRDD.map(x ->new String(x._2.getBytes(),0,x._2.getLength(),"ISO8859-1"));
        List<String> aa2=rdd3.collect();
        aa2.forEach(x-> {
            try {
                System.out.println(binary(x.getBytes("ISO8859-1"),16));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        });*/

        /*JavaRDD<String> aa3=javaRDD.map(x ->new String(x._2.getBytes(),0,x._2.getLength(),"ISO8859-1"))
                .map(x-> binary(x.getBytes("ISO8859-1"),16)
        );*/
        /*
        class MaxLength2 implements Function2<String, String,String> {
            @Override
            public String call(String s1, String s2) throws Exception {
                if(s1.length()>s2.length())  return s1;
                else return s2;
            }
        }
        String aa4=javaRDD.map(x ->new String(x._2.getBytes(),0,x._2.getLength(),"ISO8859-1"))
                .map(x-> binary(x.getBytes("ISO8859-1"),16)
                ).reduce(new MaxLength2());
        System.out.println(aa4);*/



        class findIp implements Function<Tuple2<IntWritable, BytesWritable>,String> {

            @Override
            public String call(Tuple2<IntWritable, BytesWritable> tt) throws Exception {
                String ba=new String(tt._2.getBytes(),26,8,"ISO8859-1");
                //String s=new String(ba.getBytes(),0,ba.length(),"ISO8859-1");
                String ip=binary(ba.getBytes("ISO8859-1"),16);
                if(ip.equals("c0a81f01effffffa"))
                    return binary(new String(tt._2.getBytes(),0,tt._2.getLength(),"ISO8859-1").getBytes("ISO8859-1"),16);
                    //return byteToStr(tt._2.getBytes());
                else
                    return "";
            }
        }
//        class finIp2 implements Function2<String, String,String> {
//            @Override
//            public String call(String s1, String s2) throws Exception {
//                if(s1==""||s2=="")
//            }
//        }
       // javaRDD.map(x->new ByteArray(x._2().getBytes(),10,8));
        /*List<String> list=new ArrayList<>();
        javaRDD.foreach(tt-> {
            String ba=new String(tt._2.getBytes(),26,8,"ISO8859-1");
            //String s=new String(ba.getBytes(),0,ba.length(),"ISO8859-1");
            String ip=binary(ba.getBytes("ISO8859-1"),16);
            if(ip.equals("c0a81f01effffffa"))
                list.add(binary(new String(tt._2.getBytes(),0,tt._2.getLength(),"ISO8859-1").getBytes("ISO8859-1"),16));
                //list.add(byteArrayToHexStr(tt._2.getBytes()));
        });
//        javaRDD.map(new findIp()).collect().foreach(x-> {
//                    if (x.length()!=0) list.add(x);
//                });
        System.out.println("the list size is "+list.size());
        list.forEach(x->System.out.println(x));*/
        List<Packet> list=new ArrayList<>();
        javaRDD.foreach(tt-> {
            Packet packet=(new returnPacket(tt._2)).createPacket();
            list.add(packet);
        });
        System.out.println(list.size());

/*
        class setLength implements Function<Tuple2<IntWritable,BytesWritable>,String> {
//            @Override
//            public String call(BytesWritable bytesWritable) throws Exception {
//                bytesWritable.setCapacity(bytesWritable.getSize());
//                return new String(bytesWritable.getBytes(),0,bytesWritable.getLength(),"ISO8859-1");
//            }

            @Override
            public String call(Tuple2<IntWritable, BytesWritable> intWritableBytesWritableTuple2) throws Exception {
                intWritableBytesWritableTuple2._2.setCapacity(intWritableBytesWritableTuple2._2.getLength());
                return new String(intWritableBytesWritableTuple2._2.getBytes(),0,intWritableBytesWritableTuple2._2.getLength(),"ISO8859-1");

            }
        }
        String aa5=javaRDD.map(new setLength()).map(x-> binary(x.getBytes("ISO8859-1"),16)
        ).reduce(new MaxLength2());
        System.out.println(aa5);*/
//        List<byte[]> list4;
//        System.out.println("a:");
//        javaRDD.map(x->(x._2));

        //System.out.println("b:");javaRDD.map(x -> x._2.get()).foreach(x ->System.out.println(x));

        //List<byte[]> list=javaRDD.map(x -> x._2().get()).collect();
        /*javaRDD.map(x -> x._2().get()).foreach(x->System.out.println(x));
        JavaRDD rdd=javaRDD.map(x -> x._1().get());*/

        //System.out.println("c:");rdd.foreach(x ->System.out.println(x));
        System.out.println("d:");
//        for(byte[] x : list){
//            for(byte y: x){
//                System.out.print(y+" ");
//            }
//            System.out.println();
//        }
//        for (Map.Entry<IntWritable, BytesWritable> entry: javaRDD.collectAsMap().entrySet()) {
//            System.out.println("entry.getValue()");
//        }
    }
}
