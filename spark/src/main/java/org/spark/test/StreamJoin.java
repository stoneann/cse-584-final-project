// /*
//  * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
//  */

// package org.spark.test;

// import java.io.BufferedReader;
// import java.io.IOException;
// import java.io.InputStreamReader;
// import java.util.ArrayList;
// import java.util.HashMap;
// import java.util.HashSet;
// import java.util.List;
// import java.util.regex.Pattern;

// import org.apache.hadoop.fs.HasFileDescriptor;
// import org.apache.spark.api.java.JavaPairRDD;
// import org.apache.spark.api.java.JavaRDD;
// import org.apache.spark.api.java.JavaSparkContext;
// import org.apache.spark.api.java.function.MapPartitionsFunction;
// import org.apache.spark.sql.SparkSession;
// import org.apache.spark.sql.functions;
// import org.apache.spark.sql.Encoders;
// import org.apache.spark.sql.catalyst.encoders.RowEncoder;
// import org.apache.spark.sql.types.StructType;

// import com.amazonaws.auth.AWSStaticCredentialsProvider;
// import com.amazonaws.auth.BasicAWSCredentials;
// import com.amazonaws.services.s3.AmazonS3;
// import com.amazonaws.services.s3.AmazonS3ClientBuilder;
// import com.amazonaws.services.s3.model.S3Object;

// import org.apache.spark.sql.Dataset;
// import org.apache.spark.sql.Encoder;
// import org.apache.spark.sql.Row;
// import org.apache.spark.sql.SaveMode;
// import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
// import org.apache.spark.sql.catalyst.expressions.Sequence;
// import org.apache.spark.sql.execution.aggregate.RowBasedHashMapGenerator;
// import org.apache.spark.sql.types.DataTypes;

// import scala.Function1;
// import scala.Tuple2;

// public class StreamJoin {

//     public static void main(String[] args) {
//         if (args.length < 2) {
//             System.err.println("Usage: JavaWordCount <inputFile> <outputFile>");
//             System.exit(1);
//         }

//         SparkSession spark = SparkSession
//                 .builder()
//                 .appName("SparkJob")
//                 .getOrCreate();

//         ArrayList test = new ArrayList<String>();
//         test.add("test");
//         test.add("test2");

//         JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

//         //JavaRDD<String> ans;

        
//         // JavaRDD<String> textFile = spark.read().textFile(args[0])
//         // .filter(
//         //     (org.apache.spark.api.java.function.FilterFunction<String>)
//         //     s -> {
                
//         //     }).toJavaRDD();

//         HashSet<Integer> used_partitions = new HashSet<Integer>();
//         List<String> filesToRead = new ArrayList<String>();

//         var arg5Tokens = args[5].split(",");
//         for (var token : arg5Tokens) {
//             used_partitions.add(Integer.parseInt(token));
//             filesToRead.add(args[0] + token + ".csv");
//         }

//         //Big table
//         Dataset<Row> df1 = spark.read()
//                 .option("header", "false")
//                 .csv(filesToRead.toArray(new String[filesToRead.size()]))
//                 .select("_c0", "_c1", "_c2") // Selecting the first two columns
//                 .withColumnRenamed("_c0", "big_table_rid")
//                 .withColumnRenamed("_c1", "join_column")
//                 .withColumnRenamed("_c2", "data_1");

//         //Small table
//         Dataset<Row> df2 = spark.read()
//                 .option("header", "false")
//                 .csv(args[1])
//                 .select("_c0", "_c1", "_c2")
//                 .withColumnRenamed("_c0", "small_table_rid")
//                 .withColumnRenamed("_c1", "join_column")
//                 .withColumnRenamed("_c2", "data_2")
//                 .withColumn("sm_source_file", functions.input_file_name());

//         StructType structType = new StructType();
//             structType = structType.add("small_table_rid", DataTypes.StringType, false);
//             structType = structType.add("join_column", DataTypes.StringType, false);
//             structType = structType.add("data_2", DataTypes.StringType, false);
//             structType = structType.add("sm_source_file", DataTypes.StringType, false);

//         var encoder = Encoders.row(structType);

//         var ans = df2.mapPartitions((MapPartitionsFunction<Row, Row>) s -> {
                    
//                     BasicAWSCredentials awsCreds = new BasicAWSCredentials(Secrets.ACCESS_KEY, Secrets.SECRET_KEY);
//                     AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
//                             .withRegion("us-east-2") // Set your region
//                             .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
//                             .build();
                    
                    
//                     List<Row> results = new ArrayList<>();
//                     String indexPath = args[3];
//                     String fileName = args[4];
//                     if (!s.hasNext()) {
//                         return results.iterator();
//                     }

//                     Row file = s.next();
//                     String sampleFileName = file.getAs("sm_source_file");
//                     String smallTableName = sampleFileName.substring(sampleFileName.lastIndexOf("/")+1, sampleFileName.indexOf('.'));
                    
//                     HashMap<Integer, Integer> index = new HashMap<>();

//                     try {
//                         System.err.println("S3 File: " + String.format(indexPath + "sm_source_file=%s/", smallTableName) + fileName);
//                         S3Object s3Object = s3Client.getObject("584spark-east2", String.format(indexPath + "sm_source_file=%s/", smallTableName) + fileName);
//                         BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
//                         String s3Line = reader.readLine(); // Skip header
//                         while ((s3Line = reader.readLine()) != null) {
//                             // Process data from S3
//                             var tokens = s3Line.split(",");
//                             index.put(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[0]));
//                         }
//                         while (s.hasNext()) {
//                             // Process data from small table
//                             int rid = Integer.parseInt(file.getAs("small_table_rid"));
//                             if (used_partitions.contains(index.get(rid))) {
//                                 results.add(file);
//                             }
//                             file = s.next();
//                         }
//                         int rid = Integer.parseInt(file.getAs("small_table_rid"));
//                         if (used_partitions.contains(index.get(rid))) {
//                             results.add(file);
//                         }

//                     } catch (IOException e) {
//                         // Handle exceptions
//                         System.out.println("Error reading file from S3");
//                         e.printStackTrace();
//                     }
//                     return results.iterator();
//                 }, encoder);

//         ans.drop("sm_source_file");

//         String count = "Filtered count:" + ans.count();
//         System.err.println(count);

//         Dataset<Row> result = df1.join(ans, "join_column");
        

//         result.write().mode(SaveMode.Overwrite)
//               .option("header", "true")
//               .csv(args[2]);

//         List<String> manualLog = new ArrayList<>();
//         manualLog.add("Manual log");
//         manualLog.add(count);
//         JavaRDD<String> log = sc.parallelize(manualLog);
//         log.saveAsTextFile(args[2] + "/log");

//         spark.stop();
//     }
// }