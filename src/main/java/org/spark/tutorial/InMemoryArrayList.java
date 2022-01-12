package org.spark.tutorial;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;

public class InMemoryArrayList {

    public static void main(String[] args) {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        System.setProperty("hadoop.home.dir","C:\\Hadoop");

        SparkSession sql = SparkSession.builder().appName("In Memory for testing").master("local[*]")
                .config("spark.sql.warehouse.dir","C:\\Users\\R D\\OneDrive\\Desktop\\").getOrCreate();

        List<Row> inMemory = new ArrayList<>();

        // add elements to list through rowFactory
        inMemory.add(RowFactory.create("WARN:","2021/12/31"));
        inMemory.add(RowFactory.create("ERROR:","2021/01/03"));

        StructField[] fields = new StructField[] {
                new StructField("level", DataTypes.StringType,false, Metadata.empty()),
                new StructField("date",DataTypes.StringType,false,Metadata.empty())

        };

        StructType schema = new StructType(fields);

        Dataset<Row> dataset = sql.createDataFrame(inMemory, schema);
        dataset.show();

    }

}
