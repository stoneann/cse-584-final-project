package org.spark.test;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.S3Object;

import scala.Function1;

public class GenerateIndex {
    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("Usage: CsvJoinExample <big_table_csv_path> <small_table_csv_path> <output_path>");
            System.exit(1);
        }

        String csv1Path = args[0];  // First CSV file
        String csv2Path = args[1];  // Second CSV file
        String outputPath = args[2];  // Output path

        // Create a SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("CSV Join Example")
                .getOrCreate();

        //Big table
        Dataset<Row> df1 = spark.read()
                .option("header", "false")
                .csv(csv1Path)
                .select("_c0", "_c1") // Selecting the first two columns
                .withColumnRenamed("_c0", "big_table_rid")
                .withColumnRenamed("_c1", "join_column")
                .withColumn("source_file", functions.input_file_name());

        //Small table
        Dataset<Row> df2 = spark.read()
                .option("header", "false")
                .csv(csv2Path)
                .select("_c0", "_c1")
                .withColumnRenamed("_c0", "small_table_rid")
                .withColumnRenamed("_c1", "join_column")
                .withColumn("sm_source_file", functions.input_file_name());



        // Join the two DataFrames on the join column.
        // Should be fast, since operating only with record_ids and join_column
        Dataset<Row> index = df1.join(df2, "join_column")
                    .drop("big_table_rid", "join_column")
                    .withColumn("source_file", functions.expr("substring_index(substring_index(source_file, '/', -1), '.', 1)"))
                    .withColumn("sm_source_file", functions.expr("substring_index(substring_index(sm_source_file, '/', -1), '.', 1)"));
        //                         .map((Function1<Row, Row>) row -> {
        //         String sourceFile = row.getAs("source_file");
        //         String smSourceFile = row.getAs("sm_source_file");
        //         String truncatedSourceFile = sourceFile.substring(sourceFile.lastIndexOf("/") + 1, sourceFile.lastIndexOf("."));
        //         String truncatedSmSourceFile = smSourceFile.substring(smSourceFile.lastIndexOf("/") + 1, sourceFile.lastIndexOf("."));
                
        //         return RowFactory.create(row.getAs("small_table_rid"), truncatedSourceFile, truncatedSmSourceFile);
            
        // }, RowEncoder.apply(new StructType()
        //                                 .add("small_table_rid", "string")
        //                                 .add("source_file", "string")
        //                                 .add("sm_source_file", "string")));

        // Write the joined result as CSV to the output path
        index.repartition(1).write().mode(SaveMode.Overwrite).partitionBy("sm_source_file")
              .option("header", "true")
              .csv(outputPath);

        spark.stop();
        
        // FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
        
        // for (int i = 1; i <= 10; i++) { 
        //     String filePath = String.format(outputPath + "sm_source_file=%d/", i);
        //     String fileName = fs.globStatus(new Path(filePath+"part*"))[0].getPath().getName();

        //     fs.rename(new Path(filePath+fileName), new Path(filePath+"file.csv"));
        // }   

        // BasicAWSCredentials awsCreds = new BasicAWSCredentials(Secrets.ACCESS_KEY, Secrets.SECRET_KEY);
        // AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
        //         .withRegion("us-east-2") // Set your region
        //         .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
        //         .build();
        
        // String bucketName = "584spark-east2";

        // var objects = s3Client.listObjectsV2(bucketName, outputPath.substring("s3://584spark-east2/".length()) + "sm_source_file=");
        
        // var tokens = objects.getObjectSummaries().get(0).getKey().split("/");
        // String fileName = tokens[tokens.length-1];
        // for (int i = 1; i <= 10; i++) {
        //     String filePath = String.format(outputPath + "sm_source_file=%d/", i);
        //     String destinationKeyName = filePath + "file.csv";
        //     CopyObjectRequest copyObjRequest = new CopyObjectRequest(bucketName, 
        //             filePath + fileName, bucketName, destinationKeyName);
        //     s3Client.copyObject(copyObjRequest);
        //     s3Client.deleteObject(new DeleteObjectRequest(bucketName, filePath + fileName));
        // }
    }
}
