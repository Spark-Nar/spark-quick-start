package com.java.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class RDDProgramming {
    public static void main(String[] args) {
        //--------------Spark Context
        SparkConf conf = new SparkConf()
                .setAppName("RDD Programming")
                .setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);

        //------------Creating RDD
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> intDataRdd = context.parallelize(data).cache();
        Integer counter = 0;
        System.out.println("-----------Printing data of collection---------------");
        intDataRdd.collect().forEach(System.out::println); // collect brings all data to driver node then print, without this result may be inappropriate

        //----------Accumulator for summing
        LongAccumulator accm = context.sc().longAccumulator();
        intDataRdd.foreach(x -> accm.add(x));
        System.out.println("-------Accumulator sum val "+ accm.value());

        //---------Reading File------------------
        JavaRDD<String> linesRdd = context.textFile("hdfs://localhost:9000/narendra/FileToCopyInHDFS.txt");
        // fs.defaultFS mentioned as hdfs://localhost:9000 in core-site.xml, took from there
        JavaRDD<Integer> lineLengthRdd = linesRdd.map(s -> s.length());

        lineLengthRdd.persist(StorageLevel.MEMORY_ONLY());//If we also wanted to use lineLengths again later

        int length = lineLengthRdd.reduce((a, b) -> a + b);
        System.out.println("-----------Text File Length---------------");
        System.out.println("File length "+length);

        //---------Paired RDD
        JavaRDD<String> linesRdd2 = context.textFile("hdfs://localhost:9000/narendra/FileToCopyInHDFS.txt");
        JavaPairRDD<String, Integer> pairs = linesRdd2.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a+b);

        System.out.println("-------Printing Paired RDD----------");
        //counts.foreach(System.out::println);



    }
}
