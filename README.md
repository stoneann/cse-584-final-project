# cse-584-final-project

## benchmark-datasets-generation Folder
Contains any files related to generating a dataset for benchmarking and evaluation in our project.

## benchmark-datasets Folder
Contains the actual datasets for benchmarking and evaluating our project. Added to gitignore due to size of files. Note the following naming schema:

<strong>L\<left_table_cardinality>_R\<right_table_cardinality>_M\<left_table_mapping>-\<right_table_mapping>_RS\<record_size>_S\<sorted>/\<l_or_r>/</strong>

Definitions:
* <strong>L</strong>: Left table
* <strong>R</strong>: Right table
* <strong>RS</strong>: Record Size
* <strong>left_table_cardinality</strong>: Num rows in the left table
* <strong>right_table_cardinality</strong>: Num rows in the right table
* <strong>left_table_mapping</strong>: Num rows in the left table that map to a specific row in the right table
* <strong>right_table_mapping</strong>: Num rows in the right table that map to a specific row in the left table
* <strong>record_size</strong>: Number of bytes in a record
* <strong>l_or_r</strong>: Left or Right table. L if left, R if right.
* <strong>sorted</strong>: T or F. If T, sorted based on join_id. If F, random join_ids.

## spark Folder
Contains the current spark program. Currently just a test word count program.

Running the program:

Compile App.java with Java 17, Ctrl-Shift-P -> Java: Compile Workspace on VSCode with the Java extension works for me, but as long as you get a jar in the end you should be fine.

Log into AWS https://dqanxy.signin.aws.amazon.com/console. See the GC for your login details if you haven't logged in before. Locate both the S3 bucket (584spark-east2), and the EMR console (you should see a few terminated instances). 

Upload your jar to the S3 bucket, and locate your input dataset on the S3 bucket or upload your own dataset.

Follow these instructions to set up an EMR cluster: https://medium.com/big-data-on-amazon-elastic-mapreduce/run-a-spark-job-within-amazon-emr-in-15-minutes-68b02af1ae16. Start from "Create an Amazon EMR cluster & Submit the Spark Job". Select "Set cluster size manually" and use 1 m5.xlarge for the core and 3 m5.xlarge instances for task. You can ignore step 4, EMR will automatically create a new S3 bucket for cluster logs. Select "create a service role" and "create an instance profile", and then select "All S3 buckets in this account with read and write access" for the Amazon IAM sections. Leave everything else default. You don't have to create a step while creating the cluster, you can do that after creating the cluster (for some reason, "steps" is how EMR handles running spark jobs).

Once the cluster is set up, you can configure a step. Use the same setup in the instructions, use the jar you uploaded to the S3 bucket in the application location section, and your class parameter should be org.spark.test.App (whatever has the main function). For the test spark program, the first parameter is the text file input, and the second parameter is the directory to dump the output to, same as the instructions.

## GenerateIndex.java

Generates the index. 

Argument 1: Big table location (e.g. s3://584spark-east2/datasets/L1000000_R1000000_M1-1_RS1000_SF/L/)
Argument 2: Small table location (e.g. s3://584spark-east2/datasets/L1000000_R1000000_M1-1_RS1000_SF/R/)
Argument 3: Output path (e.g. s3://584spark-east2/output-index/)

This will generate a list of csv files, one corresponding to each small table file, of the format big_table_file_number, small_table_id. Remember your output path for running the stream join job.

## StreamJoin.java

Joins using only a few partitions of the big table by using the index, the results are dumped to S3 but in practice it should be dumped to Kafka or directly to consumers.

Argument 1: Big table location (e.g. s3://584spark-east2/datasets/L1000000_R1000000_M1-1_RS1000_SF/L/)
Argument 2: Small table location (e.g. s3://584spark-east2/datasets/L1000000_R1000000_M1-1_RS1000_SF/R/)
Argument 3: Output path (e.g. s3://584spark-east2/output-join/)
Argument 4: Index path (e.g. output-index/)
Argument 5: Index file name. Due to spark limitations, renaming output files are hard so we have to use whatever default name Spark uses. Look in one of the sm_source_file directories for a file and copy the name, this file name is the exact same for all index files. (e.g. part-00000-05e3e96a-315b-4248-b13f-80e227fa257f.c000.csv)
Argument 6: List of big table file numbers to be used, separated by commas, no spaces. (e.g. 3,4,5)

Performs the join with the given big table files using the index.

# Secrets.java

This contains the AWS access key and is needed to compile streamjoin, let me know (Daniel Tian) if you need this.

# IMPORTANT

Delete your EMR as soon as you are done with it. Sucks, but these things are expensive (~1$ per hour). Also, please don't mess up my AWS console (yall have admin privileges).