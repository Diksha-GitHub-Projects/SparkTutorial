package org.spark.tutorial;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

public class FilterDatasets {
    public static void main(String[] args) {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        System.setProperty("hadoop.home.dir","C:\\Hadoop");

        SparkSession session = SparkSession.builder().appName("FilterDatasets").master("local[*]").
                config("spark.sql.warehouse.dir","C:\\Users\\R D\\OneDrive\\Desktop\\").getOrCreate();

        Dataset<Row> dataset = session.read().option("header","true").csv("C:\\Users\\R D\\OneDrive\\Desktop\\house_post.txt");
        Dataset<Row> filteredDataset = dataset.filter("id = '111' and num != '2'");
        //dataset.show();

        Column idColumn = dataset.col("id");
        Dataset<Row> id111 = dataset.filter(idColumn.equalTo(111).and(dataset.col("num").geq(1)));
       // id111.show();

        Column idWithFunctions = functions.col("id");
        dataset.filter(idWithFunctions.equalTo(111).and(dataset.col("num").notEqual(1)));

        Dataset<Temp> encoders = dataset.select(col("id"), col("num")).as(Encoders.bean(Temp.class));
        encoders.groupBy("id").agg(sum("num")).show();
    }
}
