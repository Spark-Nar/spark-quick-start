package com.java.spark;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class SimpleApp {
    public static void main(String[] args) {
        String logFileLocation = "D:\\Soft\\BigData\\spark-2.4.4-bin-hadoop2.7\\README.md";
        SparkSession session = SparkSession
                .builder()
                .appName("Simple Application")
                .config("spark.master", "local")
                /*.config("spark.testing.memory", "2147480000")*/
                .getOrCreate();
        Dataset<String> logData = session.read().textFile(logFileLocation).cache();

        long numAs = logData.filter((FilterFunction<String>)  s -> s.contains("a")).count();
        long numBs = logData.filter((FilterFunction<String>)  s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        session.stop();

    }

}
