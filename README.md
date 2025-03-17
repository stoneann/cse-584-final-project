# cse-584-final-project

## benchmark-datasets-generation Folder
Contains any files related to generating a dataset for benchmarking and evaluation in our project.

## benchmark-datasets Folder
Contains the actual datasets for benchmarking and evaluating our project.

## spark Folder
Contains the current spark program. Currently just a test word count program.

Running the program:

Compile App.java with Java 17, Ctrl-Shift-P -> Java: Compile Workspace on VSCode with the Java extension works for me, but as long as you get a jar in the end you should be fine.

Log into AWS https://dqanxy.signin.aws.amazon.com/console. See the GC for your login details if you haven't logged in before. Locate both the S3 bucket (584spark-east2), and the EMR console (you should see a few terminated instances). 

Upload your jar to the S3 bucket, and locate your input dataset on the S3 bucket or upload your own dataset.

Follow these instructions to set up an EMR cluster: https://medium.com/big-data-on-amazon-elastic-mapreduce/run-a-spark-job-within-amazon-emr-in-15-minutes-68b02af1ae16. Start from "Create an Amazon EMR cluster & Submit the Spark Job". Select "Set cluster size manually" and use 1 m5.xlarge for the core and 3 m5.xlarge instances for task. You can ignore step 4, EMR will automatically create a new S3 bucket for cluster logs. Select "create a service role" and "create an instance profile", and then select "All S3 buckets in this account with read and write access" for the Amazon IAM sections. Leave everything else default. You don't have to create a step while creating the cluster, you can do that after creating the cluster (for some reason, "steps" is how EMR handles running spark jobs).

Once the cluster is set up, you can configure a step. Use the same setup in the instructions, use the jar you uploaded to the S3 bucket in the application location section, and your class parameter should be org.spark.test.App (whatever has the main function). For the test spark program, the first parameter is the text file input, and the second parameter is the directory to dump the output to, same as the instructions.

# IMPORTANT

Delete your EMR as soon as you are done with it. Sucks, but these things are expensive (~1$ per hour). Also, please don't mess up my AWS console (yall have admin privileges).