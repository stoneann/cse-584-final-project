// package com.example;

// import org.apache.spark.sql.Dataset;
// import org.apache.spark.sql.Row;
// import org.apache.spark.sql.SparkSession;

// public class Check {
//   public static void main(String[] args) {
//     if (args.length < 2) {
//       System.err.println("Usage: Check <generatedOutputFile> <correctOutputFile>");
//       System.exit(1); 
//     }
//     SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();

//     Dataset<Row> df = spark.read().csv(path);
    
//     // Correct table
//     // Dataset<Row> df1 = spark.read()
//     //         .option("header", "false")
//     //         .csv(args[1])
//     //         .select("_c0", "_c1", "_c2") // Selecting the first two columns
//     //         .withColumnRenamed("_c0", "big_table_rid")
//     //         .withColumnRenamed("_c1", "join_column")
//     //         .withColumnRenamed("_c2", "data_1");

//     spark.stop();
//   }
// }
// //spark://aryan-XPS-13-9370:7077 