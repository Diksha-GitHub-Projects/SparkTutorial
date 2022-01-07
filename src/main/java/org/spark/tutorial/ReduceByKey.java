package org.spark.tutorial;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class ReduceByKey {
    //Reduce by key is better than group by key , groupbyKey has performance issue
    // ReduceBykey <T,U> GroupByKey <T , Iterable>
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("reduce by key spark app").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> data = sc.parallelize(Arrays.asList("WARN: hello","WARN: hello2","DEBUG: hello3"));
        JavaPairRDD<String,String> splittedData = data.mapToPair( x ->
        {
            String[] values = x.split(":");
            return new Tuple2<>(values[0],values[1]);
        });

        splittedData.collect().forEach(x -> System.out.println(x._1 + x._2));
        JavaPairRDD<String, Integer> reducedData = splittedData.mapToPair(x-> new Tuple2<>(x._1,1)).reduceByKey((x, y)-> x+y);
        reducedData.collect().forEach(x-> System.out.println(x._1 +" "+ x._2));

        sc.close();

    }
}
