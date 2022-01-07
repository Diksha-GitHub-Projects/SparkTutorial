package org.spark.tutorial;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class TuplesExample {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("spark with tuples").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> data = sc.parallelize(Arrays.asList(2,3,4,6));
        JavaRDD<Tuple2<Integer,Double>> tuples = data.map(x -> new Tuple2<>(x,Math.sqrt(x)));
        tuples.collect().forEach(System.out::println);
        sc.close();
    }
}
