import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvJoinExample {
    public static void main(String[] args) {
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

        Dataset<Row> df1 = spark.read()
                .option("header", "false")
                .csv(csv1Path)
                .select("_c0", "_c1") // Selecting the first two columns
                .withColumnRenamed("_c0", "big_table_rid")
                .withColumnRenamed("_c1", "join_column")
                .withColumn("source_file", functions.input_file_name());

        Dataset<Row> df2 = spark.read()
                .option("header", "false")
                .csv(csv2Path)
                .select("_c0", "_c1")
                .withColumnRenamed("_c0", "small_table_rid")
                .withColumnRenamed("_c1", "join_column");

        // Join the two DataFrames on the join column.
        // Should be fast, since operating only with record_ids and join_column
        Dataset<Row> joined = df1.join(df2, "join_column");

        // Write the joined result as CSV to the output path
        joined.write()
              .option("header", "true")
              .csv(outputPath);

        spark.stop();
    }
}
