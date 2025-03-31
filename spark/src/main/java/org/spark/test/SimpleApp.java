import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class SimpleApp {
  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: AccuracyChecker <generatedOutputFile> <correctOutputFile>");
      System.exit(1); 
    }
    SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();

    


    // String logFile = "YOUR_SPARK_HOME/README.md"; // Should be some file on your system
    // Dataset<String> logData = spark.read().textFile(logFile).cache();



    // long numAs = logData.filter(s -> s.contains("a")).count();
    // long numBs = logData.filter(s -> s.contains("b")).count();

    // System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

    spark.stop();
  }
}