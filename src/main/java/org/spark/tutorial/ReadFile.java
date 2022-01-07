package org.spark.tutorial;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Arrays;

/** ignore wintutil.exe exception , we are getting this because of sc.textfile
 * but if you really dont want to see then set hadoop home
 */
public class ReadFile {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        System.setProperty("hadoop.home.dir","C:\\Hadoop");

        SparkConf conf = new SparkConf().setAppName("read file through spark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> readFile = sc.textFile("C:\\Users\\R D\\OneDrive\\Desktop\\house_post.txt");
        readFile.flatMap(x -> Arrays.asList(x.split(",")).iterator()).collect().forEach(x -> System.out.println(x));


    }
}
