/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */

package org.spark.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.spark.test.Secrets;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class GenerateIndex {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: JavaWordCount <inputFile> <outputFile>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkJob")
                .getOrCreate();

        ArrayList test = new ArrayList<String>();
        test.add("test");
        test.add("test2");

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        JavaPairRDD<String,String> files = sc.wholeTextFiles(args[2]);

        JavaRDD<String> ans;

        // JavaRDD<String> textFile = spark.read().textFile(args[0])
        // .filter(
        //     (org.apache.spark.api.java.function.FilterFunction<String>)
        //     s -> {
                
        //     }).toJavaRDD();
        ans = files.mapPartitions((org.apache.spark.api.java.function.FlatMapFunction<java.util.Iterator<Tuple2<String,String>>, String>)
                s -> {
                    
                    BasicAWSCredentials awsCreds = new BasicAWSCredentials(Secrets.ACCESS_KEY, Secrets.SECRET_KEY);
                    AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                            .withRegion("us-east-2") // Set your region
                            .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                            .build();
                    
                    
                    List<String> results = new ArrayList<>();
                    String s3Key = args[0];
                    
                    try {
                        S3Object s3Object = s3Client.getObject("584spark-east2", s3Key);
                        BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
                        String s3Line;
                        while ((s3Line = reader.readLine()) != null) {
                            // Process data from S3
                            results.add("Processed line " + s3Line);
                        }
                        while (s.hasNext()) {
                            Tuple2<String,String> file = s.next();
                            results.add("Processed file " + file._1);
                        }
                    } catch (IOException e) {
                        // Handle exceptions
                        System.out.println("Error reading file from S3");
                        e.printStackTrace();
                    }
                    return results.iterator();
                });

        ans.saveAsTextFile(args[1]);
    }
}