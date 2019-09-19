package com.java.spark;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import javax.xml.crypto.Data;
import java.util.Arrays;
import java.util.Collections;

public class DataFrameProgramming {
    public static void main(String[] args) {
        SparkSession session = SparkSession
                .builder()
                .appName("Application")
                .config("spark.master", "local")
                /*.config("spark.testing.memory", "2147480000")*/
                .getOrCreate();


        Dataset<Row> df = session.read().json("hdfs://localhost:9000/narendra/people.json");

        System.out.println("-------Show Data Frame----------");
        df.show();
        System.out.println("-------Print Schema----------");
        df.printSchema();
        System.out.println("--------Count------------");
        System.out.println(df.count());
        System.out.println("--------Showing Column-------- ");
        df.select("name").show();
        df.select(functions.col("name"), functions.col("age").plus(1)).show();
        df.filter(functions.col("age").gt(20)).show();
        df.groupBy("age").count().show();

        System.out.println("----------Create Temp View--------------");
        df.createOrReplaceTempView("peopleTempView");
        Dataset<Row> sqlDf = session.sql("SELECT * from peopleTempView");
        sqlDf.show();

        System.out.println("----------Create Global Temp View--------------");
        try{
            df.createGlobalTempView("peopleGlobalTempView");
        }catch (AnalysisException ex){
            ex.printStackTrace();
        }

        session.sql("select * from global_temp.peopleGlobalTempView");
        session.newSession().sql("select * from global_temp.peopleGlobalTempView").show();


        System.out.println("-----------Creating DataSet-------------------");
        //Datasets are similar to RDDs, however, instead of using Java serialization or Kryo they use a specialized Encoder to serialize the objects for processing or transmitting over the network

        Person person = new Person("narendra", 29);
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> personDataset = session.createDataset(Collections.singletonList(person), personEncoder);
        personDataset.show();

        System.out.println("--------Primitive Encoders-----------------");
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> integerDataset = session.createDataset(Arrays.asList(1,2,3), integerEncoder);
        Dataset<Integer> transformedIntDataset = integerDataset.map( (MapFunction<Integer, Integer>)  value -> value + 1, integerEncoder);
        transformedIntDataset.collect();

        Dataset<Person> peopleDs = session.read().json("hdfs://localhost:9000/narendra/people.json").as(personEncoder);
        peopleDs.show();

        System.out.println("----------Interoperating with RDDs----------");
        //session.read().te

        session.stop();

    }
}
