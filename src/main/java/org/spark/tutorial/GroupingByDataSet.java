package org.spark.tutorial;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class GroupingByDataSet {
    public static void main(String[] args) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
    System.setProperty("hadoop.home.dir","C:\\Hadoop");

    SparkSession sql = SparkSession.builder().appName("In Memory for testing").master("local[*]")
            .config("spark.sql.warehouse.dir", "C:\\Users\\R D\\OneDrive\\Desktop\\").getOrCreate();

    List<Row> inMemory = new ArrayList<>();

    // add elements to list through rowFactory
        inMemory.add(RowFactory.create("WARN:","2021-12-31"));
        inMemory.add(RowFactory.create("ERROR:","2021-01-03"));
        inMemory.add(RowFactory.create("WARN:","2021-09-08"));
        inMemory.add(RowFactory.create("WARN:","2018-09-09"));

    StructField[] fields = new StructField[]{
            new StructField("level", DataTypes.StringType, false, Metadata.empty()),
            new StructField("date", DataTypes.StringType, false, Metadata.empty())

    };

    StructType schema = new StructType(fields);
    Dataset<Row> dataset = sql.createDataFrame(inMemory, schema);
    dataset.createOrReplaceTempView("list");

    Dataset<Row> dataset_max= sql.sql("select max(date),level from list group by level");
    dataset_max.show();

    sql.sql("select collect_set(date),level from list group by level").show();
    sql.sql("select date_format(date,'MMM') from list").show();
    sql.sql("select level,cast(date_format(date,'M') as int) as month,count(1) from list group by level,cast(date_format(date,'M') as int)").show(false);

    dataset.selectExpr("level","date_format(date,'MMM') as month").show();

    //copy of all sql functions in functions class
    dataset.select(functions.col("level"), functions.col("date")).groupBy("level").agg(functions.collect_list(functions.col("date"))).show();
           // .map((MapFunction<String,String>) s -> s.toLowerCase(Locale.ROOT),Encoders.STRING()).show();

   // pivot table is row and columns created by table data , it is used when we group by on two columns

        dataset.groupBy(functions.col("level")).pivot("date").count().na().fill(0).show();



    }
}
