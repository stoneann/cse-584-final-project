package org.spark.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.HashMap;
import java.util.List;

public class Checker {
  public static void main(String[] args) {
    // if (args.length < 2) {
    //   System.err.println("Usage: Check <generatedOutputFile> <correctOutputFile>");
    //   System.exit(1); 
    // }
    String generatedOutputFile = "/home/aryan/584/cse-584-final-project/benchmark-datasets/L5_R5_M1-1_RS1000_SF/JOIN/5.csv"; // args[0];
    String correctOutputFile = "/home/aryan/584/cse-584-final-project/benchmark-datasets/L5_R5_M1-1_RS1000_SF/JOIN/1.csv";  // Output path // args[1];

    SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();

    // Read in Correct Rows
    Dataset<Row> correctDf = spark.read()
        .option("header", "true")
        .csv(correctOutputFile);
    
    // correctDf.show();
    
    // Read in Generated Rows
    Dataset<Row> generatedDf = spark.read()
        .option("header", "true")
        .csv(generatedOutputFile);

    // Check - NOTE: only works for 1 - 1 mappings

    // 1. Sort both datatables
    generatedDf.orderBy("join_column");
    correctDf.orderBy("join_column");

    // 2. Iterate through generated dataset and ensure each join_column data is correct
    List<Row> gen_rows = generatedDf.collectAsList();
    List<Row> correct_rows = correctDf.collectAsList();

    if (correct_rows.size() != gen_rows.size()) {
        System.err.println("ERROR: wrong sizes. Correct Size: " + correct_rows.size() + ". Generated Size: " + gen_rows.size());
        spark.stop();
        return;
    }

    for (int i = 0; i < correct_rows.size(); i++ ) {
        Row gen_row = gen_rows.get(i);
        Row correct_row = correct_rows.get(i);
        
        boolean correctJoinColumn = gen_row.getAs("join_column").equals(correct_row.getAs("join_column"));
        boolean correct_big_table_rid = gen_row.getAs("big_table_rid").equals(correct_row.getAs("big_table_rid"));
        boolean correct_data_1 = gen_row.getAs("data_1").equals(correct_row.getAs("data_1"));
        boolean correct_small_table_rid = gen_row.getAs("small_table_rid").equals(correct_row.getAs("small_table_rid"));
        boolean correct_data_2 = gen_row.getAs("data_2").equals(correct_row.getAs("data_2"));

        if (!correctJoinColumn || !correct_big_table_rid || !correct_data_1 || !correct_small_table_rid || !correct_data_2) {
            System.err.println("ERROR: Content of rows do not match at join_column of correctDf " + correct_row.getAs("join_column") + ". \nCorrect Row: " + correct_row.toString() + ". \nGenerated Row: " + gen_row.toString());
            spark.stop();
            return;
        }
    }

    System.out.println("PASS - Correct File Name: " + correctOutputFile + ". Generated File Name: " + generatedOutputFile + ".");
    spark.stop();
  }
}
//spark://aryan-XPS-13-9370:7077 