package org.spark.tutorial;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SortByKey {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        System.setProperty("hadoop.home.dir", "C:\\Hadoop");

        SparkConf conf = new SparkConf().setAppName("read file through spark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> readFile = sc.textFile("C:\\Users\\R D\\OneDrive\\Desktop\\house_post.txt");
        JavaRDD<String> removeChars = readFile.map(word -> word.replaceAll("[^a-zA-Z\\s]", "").trim()).filter(word -> word.length() > 0);

        System.out.println(removeChars.getNumPartitions());
        JavaPairRDD<String,Long> pairRDD1 = removeChars.mapToPair( word -> new Tuple2<>(word,1L));
        //coalesce brings all the data in the mentioned no. of partitions in thsi case in 1 partition
        pairRDD1.reduceByKey((x,y) -> x+y).sortByKey(true).coalesce(1).collect().forEach(System.out::println);
        //Coalesce is a way to reduce the number of partition , use only when you know data is small
        // else will get outofmemory error

        // collect send data to driver and loads into its memory , so be sure data is small for collect
        // for each is a exceptional case in a multithreaded env

        // no need to worry about shuffling and partitioning of data in spark as it is taken care by spark ,
        // need this information only in case of performance measurement
        sc.close();
    }
    }
