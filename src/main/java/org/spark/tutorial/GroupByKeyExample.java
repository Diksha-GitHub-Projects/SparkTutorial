package org.spark.tutorial;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

public class GroupByKeyExample {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("reduce by key spark app").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> data = sc.parallelize(Arrays.asList("WARN: hello", "WARN: hello2", "DEBUG: hello3"));
        data.mapToPair(x -> new Tuple2<>(x.split(":")[0],x.split(":")[1]))
                .groupByKey().collect().forEach(x -> System.out.println(x._1 + " "+new ArrayList<String>((Collection<? extends String>) x._2)));

        data.flatMap(x -> Arrays.asList(x.split(" ")).iterator()).filter( x -> x.contains("WARN:")).collect().forEach(System.out::println);
    }


}
