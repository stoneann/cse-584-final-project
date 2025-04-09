package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class App {
  public static void main(String[] args) {
    System.out.println(" -- HELLO WORLD --");
    String logFile = "src/main/java/com/example/testfile.txt"; // Should be some file on your system
    SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
    Dataset<String> logData = spark.read().textFile(logFile).cache();
    // long numAs = 0;
    // long numBs = 0;
    long numAs = logData.filter((org.apache.spark.api.java.function.FilterFunction<String>)s -> s.contains("a")).count();
    long numBs = logData.filter((org.apache.spark.api.java.function.FilterFunction<String>)s -> s.contains("b")).count();

    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

    spark.stop();
  }
}
//spark://aryan-XPS-13-9370:7077 