package org.spark.tutorial

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.{max, sum}
//Java also have functions
import org.apache.spark.sql.functions.{col,udf}

object SparkPractice {

  def main(args:Array[String]){


    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val caseClassschema = Encoders.product[Person].schema

    val readFile = spark.read.options(Map("header"->"true","inferSchema" -> "true"))
      .csv("C:\\Hadoop\\People.csv")

    val readFile2 = spark.read.schema(caseClassschema).csv("C:\\Hadoop\\People.csv")

    println(" ----> ")

    readFile2.show()

    //readFile.explain();
    readFile.show(2)
    readFile.printSchema()

    readFile.foreach( x=> println(x))

    val x = readFile.collectAsList();

    readFile.select("name").show()

    readFile.where("name == 'Diksha' and  age > 12").show()

    println("---")

    readFile.select("name","age").filter("age> 12").show()

    readFile.count()

    readFile.groupBy("name").agg(sum("age").as("sum"),
      max("age").as("max")).show()

    readFile.createTempView("people")

    spark.sql("select * from People").show()

    spark.udf.register("strlen", (s: String) => s.length)

    spark.sql("select strlen(age) as age_length from People").show()

    val emp = Seq((1,"Smith",-1,"2018",10,"M",3000),
      (2,"Rose",1,"2010",20,"M",4000),
      (3,"Williams",1,"2010",10,"M",1000),
      (4,"Jones",2,"2005",10,"F",2000),
      (5,"Brown",2,"2010",30,"",-1),
      (6,"Brown",2,"2010",50,"",-1)
    )
    val empColumns = Seq("emp_id","name","superior_emp_id","branch_id","dept_id","gender","salary")


    import spark.sqlContext.implicits._

    val empDF = emp.toDF(empColumns:_*)
    val dept = Seq(("Finance",10,"2018"),
      ("Marketing",20,"2010"),
      ("Marketing",20,"2018"),
      ("Sales",30,"2005"),
      ("Sales",30,"2010"),
      ("IT",50,"2010")
    )

    val deptColumns = Seq("dept_name","dept_id","branch_id")
    val deptDF = dept.toDF(deptColumns:_*)

    empDF.as("e1").join(deptDF.as("d1"),col("e1.dept_id") === col("d1.dept_id") &&
      empDF("branch_id") === deptDF("branch_id"),"inner")
      .show(false)

    empDF.as("e1").join(deptDF.as("d1"), empDF("dept_id") === deptDF("dept_id") ||
      empDF("branch_id") === deptDF("branch_id"),"inner")
      .show(false)
  }

  case class Person(name: String, age: Long)

}
