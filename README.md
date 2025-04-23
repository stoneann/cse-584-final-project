# cse-584-final-project

## benchmark-datasets-generation Folder
Contains any files related to generating a dataset for benchmarking and evaluation in our project. It also contains one small sample dataset just for professor and programmer visibility. The program is written in python and takes the following arguments. 

Usage: generate.py [OPTIONS] 

  A program to generate a dataset 

Options: <br />
  -ltc, --left-table-cardinality INTEGER <br />
                                  Cardinality of Left Table <br />
  -rtc, --right-table-cardinality INTEGER <br />
                                  Cardinality of Right Table <br />
  -ltm, --left-table-mapping INTEGER <br />
                                  Number of records in left table that joins to right. <br />
  -rtm, --right-table-mapping INTEGER <br />
                                  Number of records in right table that joins to left. <br />
  -rbs, --record-byte-size INTEGER <br />
                                  Size of an individual record in bytes. <br />
  -s, --sorted BOOLEAN  <br />
                                  If true, dataset will be sorted by join index. If false, datset will be random. <br />
  --help  <br />
                                  Show this message and exit. <br />

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

## presto Folder
This folder contains the code for running presto. We ran presto using a java job. The pom.xml is the maven build dependencies. The src folder is the source code for the join. The target folder is the most recent build of the presto java project.

## spark Folder
Contains the current spark program. Currently just a test word count program.

Running the program:

Compile App.java with Java 17, Ctrl-Shift-P -> Java: Compile Workspace on VSCode with the Java extension works for me, but as long as you get a jar in the end you should be fine.

Log into AWS https://dqanxy.signin.aws.amazon.com/console. See the GC for your login details if you haven't logged in before. Locate both the S3 bucket (584spark-east2), and the EMR console (you should see a few terminated instances). 

Upload your jar to the S3 bucket, and locate your input dataset on the S3 bucket or upload your own dataset.

Follow these instructions to set up an EMR cluster: https://medium.com/big-data-on-amazon-elastic-mapreduce/run-a-spark-job-within-amazon-emr-in-15-minutes-68b02af1ae16. Start from "Create an Amazon EMR cluster & Submit the Spark Job". Select "Set cluster size manually" and use 1 m5.xlarge for the core and 3 m5.xlarge instances for task. You can ignore step 4, EMR will automatically create a new S3 bucket for cluster logs. Select "create a service role" and "create an instance profile", and then select "All S3 buckets in this account with read and write access" for the Amazon IAM sections. Leave everything else default. You don't have to create a step while creating the cluster, you can do that after creating the cluster (for some reason, "steps" is how EMR handles running spark jobs).

Once the cluster is set up, you can configure a step. Use the same setup in the instructions, use the jar you uploaded to the S3 bucket in the application location section, and your class parameter should be org.spark.test.App (whatever has the main function). For the test spark program, the first parameter is the text file input, and the second parameter is the directory to dump the output to, same as the instructions.

# Setup CloudWatch Metrics

When creating a cluster, after selecting your application bundle (e.g. Spark, Presto), in the checkboxes below also select "AmazonCloudWatchAgent 1.300032.2". Finish creating the cluster as normal.

After the cluster has finished setting up, go to Configurations > Instance Group Configurations, select any of the instance groups and click Reconfigure. Select Edit JSON and paste the below JSON: 

[
  {
    "Classification": "emr-metrics",
    "Configurations": [
      {
        "Classification": "emr-system-metrics",
        "Configurations": [
          {
            "Classification": "cpu",
            "Properties": {
              "drop_original_metrics": "cpu_usage_guest",
              "metrics": "cpu_usage_active,cpu_usage_guest,cpu_usage_guest_nice,cpu_usage_idle,cpu_usage_iowait,cpu_usage_irq,cpu_usage_nice,cpu_usage_softirq,cpu_usage_steal,cpu_usage_system,cpu_usage_user",
              "metrics_collection_interval": "10"
            }
          },
          {
            "Classification": "mem",
            "Properties": {
              "metrics": "mem_active, mem_available, mem_available_percent, mem_free,mem_inactive,mem_total,mem_used,mem_used_percent,mem_buffered,mem_cached"
            }
          },
          {
            "Classification": "disk",
            "Properties": {
              "drop_original_metrics": "",
              "metrics": "disk_used_percent, disk_free,disk_total,disk_used",
              "resources": "/,/mnt"
            }
          }
        ],
        "Properties": {
          "metrics_collection_interval": "10"
        }
      }
    ],
    "Properties": {}
  }
]

Before clicking submit, also check apply to all active instance groups so that this configuration will apply to all nodes. If you forget to do this, you can also just reconfigure every instance group one by one using the same process.

Finally, under Properties > Permissions > EC2 Instance Profile, you should see a name like "AmazonEMR-InstanceProfile-20250315T173317". This is an IAM role, and you need to give it permissions to write to CloudWatch. Leave EMR and go to the IAM dashboard, go to roles and find the role with the exact name. Then go to Permissions > Add Permissions > Attach Policies, search up "CloudWatchFullAccess", and select both CloudWatchFullAccess and CloudWatchFullAccessV2, and click Add Permissions.

Done! You should see the posted metrics in the cluster's EMR dashboard, under Monitoring > CloudWatch agent - new, in particular the node memory usage and CPU utilization.

## GenerateIndex.java

Generates the index. 

Argument 1: Big table location (e.g. s3://584spark-east2/datasets/L1000000_R1000000_M1-1_RS1000_SF/L/)
Argument 2: Small table location (e.g. s3://584spark-east2/datasets/L1000000_R1000000_M1-1_RS1000_SF/R/)
Argument 3: Output path (e.g. s3://584spark-east2/output-index/)

This will generate a list of csv files, one corresponding to each small table file, of the format big_table_file_number, small_table_id. Remember your output path for running the stream join job.

The tail end of this task is slow as in order to output one index file per small table file, the current infrastructure requires all data to go through one node, as shown by repartition(1) in line 83. This removes parallelization and makes this step slow, this can be mitigated by instead outputting more than one index file per small table file, by changing it to like repartition(10) for 10 index files per small table file. This slightly changes the API for StreamJoin. 

## StreamJoin.java

Joins using only a few partitions of the big table by using the index, the results are dumped to S3 but in practice it should be dumped to Kafka or directly to consumers.

Argument 1: Big table location (e.g. s3://584spark-east2/datasets/L1000000_R1000000_M1-1_RS1000_SF/L/)
Argument 2: Small table location (e.g. s3://584spark-east2/datasets/L1000000_R1000000_M1-1_RS1000_SF/R/)
Argument 3: Output path (e.g. s3://584spark-east2/output-join/)
Argument 4: Index path (e.g. output-index/) Note: uses s3 keys not ARNs, so no s3:///584spark-east2/ prefix.
Argument 5: Index file name. Due to spark limitations, renaming output files are hard so we have to use whatever default name Spark uses. Look in one of the sm_source_file directories for a file and copy the name, this file name is the exact same for all index files. (e.g. part-00000-05e3e96a-315b-4248-b13f-80e227fa257f.c000.csv).

In the case that you made GenerateIndex output more than one index file per small-table file, concatenate all file names and separate with commas, no spaces. (e.g. part-00000-05e3e96a-315b-4248-b13f-80e227fa257f.c000.csv,part-00000-05e3e96a-315b-4248-b13f-80e227fa257f.c000.csv,part-00000-05e3e96a-315b-4248-b13f-80e227fa257f.c000.csv)

Argument 6: Starting big table file.
Argument 7: Ending big table file. (e.g., arg6=10 and arg7=20 means reading files 10,11,12,13,14,15,16,17,18,19).
Argument 8: Starting small table file.
Argument 9: Ending small table file. (e.g. arg8=1 and arg9=1001 means reading the first 1000 files of the small table (aka all small table files in the large dataset scenario)).

Performs the join with the given big table files using the index.

# Secrets.java

This contains the AWS access key and is needed to compile streamjoin, let me know (Daniel Tian) if you need this.

# IMPORTANT

Delete your EMR as soon as you are done with it. Sucks, but these things are expensive (~1$ per hour). Also, please don't mess up my AWS console (yall have admin privileges).