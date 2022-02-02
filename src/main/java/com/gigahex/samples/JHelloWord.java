package com.gigahex.samples;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import java.util.Arrays;

public class JHelloWord {

    public static void main(String[] args) throws Exception {

        //Initialize the spark session
        SparkSession spark = SparkSession.builder()
                .appName("count-words-java")
                .getOrCreate();

        //Create a dataset by reading the input file
        Dataset<String> text = spark.read().textFile("/Users/shad/hello.in");
        Dataset<String> words = text
                .flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" "))
                                .iterator(),
                        Encoders.STRING()
                );

        words.filter((FilterFunction<String>) word -> word.length() > 2)
                .write().text("/Users/shad/hello-jout");
        spark.stop();
    }

}
