package org.spark.tutorial;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Joins {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        System.setProperty("hadoop.home.dir","C:\\Hadoop");

        SparkConf conf = new SparkConf().setAppName("joins").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer,Integer>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1,3));
        data1.add(new Tuple2<>(3,4));

        List<Tuple2<Integer,String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1,"spark1"));
        data2.add(new Tuple2<>(5,"spark2"));

        // incase of tuples always pairs
        JavaPairRDD<Integer, Integer> parData1 = sc.parallelizePairs(data1);
        JavaPairRDD<Integer, String> parData2 = sc.parallelizePairs(data2);
        JavaPairRDD<Integer, Tuple2<Integer, String>> data3 = parData1.join(parData2);
        //data3.collect().forEach(System.out::println);

        // left outer join , output will be 2 rows
       JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> data4 = parData1.leftOuterJoin(parData2);
       data4.collect().forEach( x -> System.out.println(x._2._2.orElse("blank")));

       JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> data5 = parData1.rightOuterJoin(parData2);
       data5.collect().forEach(x -> System.out.println(x._2._1.orElse(0)));

       JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> data6 = parData1.fullOuterJoin(parData2);
       data6.collect().forEach(x -> System.out.println(x._2._1().orElse(0)));

       JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> data7 = parData1.cartesian(parData2);
       data7.collect().forEach(x -> System.out.println(x._1._1() + " "+x._2._2()));

       sc.close();
    }
}
