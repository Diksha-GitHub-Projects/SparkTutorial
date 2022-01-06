package org.spark.tutorial;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        List<Integer> addData = new ArrayList<>(Arrays.asList(2, 3, 4, 5, 6, 7));
        // local[*] use all cores
        SparkConf conf = new SparkConf().setAppName("sparkApplication").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(addData);
        Integer reduceVal = rdd.reduce((x,y) -> x+y);
        System.out.println(reduceVal);

        Double countRDD = rdd.map( x-> new Double(1)).reduce((x,y)-> x+y);
        System.out.println(countRDD);

        // if function takes only one parameter then no need to write x -> System.out.println(x)
        JavaRDD<Double> mapRDD = rdd.map(x -> Math.sqrt(x));
        // multiple CPUs or Cores and since println is not serializable
        //mapRDD.foreach(System.out::println);

        //collect data from all nodes and load in the JVM on which I am running the program
        mapRDD.collect().forEach(System.out::println);

        sc.close();
    }
}
