package org.spark.tutorial;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Datasets {

    public static void main(String[] args) {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        System.setProperty("hadoop.home.dir","C:\\Hadoop");

        // sparksql create sparksession not sparkconf or java
        SparkSession session = SparkSession.builder().appName("spark sql programme").master("local[*]")
                .config("spark.sql.warehouse.dir","D:\\spark-sql").getOrCreate();

        // by default sparks considers no headers
        // dataset is representation of data , it is like table which has rows
        Dataset<Row> dataset = session.read().option("header",true).csv("C:\\Users\\R D\\OneDrive\\Desktop\\house_post.txt");
        System.out.println(dataset.count());
        Row firstRowData = dataset.first();

        int value = Integer.parseInt(firstRowData.get(0).toString());
        // getAS output in String
        int sValue = Integer.parseInt(firstRowData.getAs("id"));
        System.out.println(sValue);
        System.out.println(value);
        dataset.show();

        // RDDs are still there but inside dataset

    }

}
