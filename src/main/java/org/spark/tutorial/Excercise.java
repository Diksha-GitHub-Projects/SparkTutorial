package org.spark.tutorial;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;
import scala.reflect.internal.pickling.UnPickler;
import sun.misc.InnocuousThread;

import java.util.Scanner;

public class Excercise {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        System.setProperty("hadoop.home.dir","C:\\Hadoop");

        SparkConf conf = new SparkConf().setAppName("excercise").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Integer,Integer> data = sc.textFile("C:\\Users\\R D\\OneDrive\\Desktop\\house_post.txt")
                .mapToPair( line -> {
                    String[] words = line.split(",");
                    return new Tuple2<Integer,Integer>(new Integer(words[0]),new Integer(words[1]));
                });

        JavaPairRDD<Integer,Integer> data1 = sc.textFile("C:\\Users\\R D\\OneDrive\\Desktop\\house_post.txt")
                .mapToPair( line -> {
                    String[] words = line.split(",");
                    return new Tuple2<Integer,Integer>(new Integer(words[0]),new Integer(words[1]));
                });

        data  = data.distinct();
       // data.collect().forEach( x -> System.out.println(x));

       data =  data.mapToPair(x -> new Tuple2<>(x._2, x._1));

       JavaPairRDD<Integer,Tuple2<Integer,Integer>> data2 = data.join(data1);

       // optional by leftouterjoin,righouterjoin and fullouterjoin

       JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<Integer>>> data3 = data.fullOuterJoin(data1);

        JavaPairRDD<Integer,Integer> reducedCounts = data.mapToPair(x -> new Tuple2<Integer,Integer>(x._2 , 1 ));

        JavaPairRDD<Integer,Integer> values = reducedCounts.reduceByKey( (val1,val2) -> val1 + val2);

        values.collect().forEach(x -> System.out.println(x));

        JavaPairRDD<Integer, Integer> data5 = values.mapValues(x -> x + 10);

        data5.collect().forEach(x -> System.out.println(x));
      // difference between reduceByKey and reduce -> reduce give one value and reducebyKey ( Key, count)

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        sc.close();
    }
}
