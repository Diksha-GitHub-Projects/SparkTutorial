package org.spark.tutorial;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkTableView {
    public static void main(String[] args) {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        System.setProperty("hadoop.home.dir","C:\\Hadoop");

        SparkSession session = SparkSession.builder().appName("tableView").master("local[*]")
                .config("spark.sql.warehouse.dir","C:\\Users\\R D\\OneDrive\\Desktop\\").getOrCreate();

        Dataset<Row> dataset = session.read().option("header",true).option("inferSchema",true).option("delimiter",",")
                .csv("C:\\Users\\R D\\OneDrive\\Desktop\\house_post.txt");

        // creating table in memory
        dataset.createOrReplaceTempView("house_post");

        Dataset<Row> sqlDataset = session.sql("select max(id) from house_post");

        sqlDataset.show();


    }
}
