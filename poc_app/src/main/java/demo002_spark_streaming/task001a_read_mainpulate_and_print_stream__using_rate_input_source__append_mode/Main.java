package demo002_spark_streaming.task001a_read_mainpulate_and_print_stream__using_rate_input_source__append_mode;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

public class Main {
    public static void main(String[] args) throws StreamingQueryException {

        //Create Spark Session
        //this consumer will read messages from queue using spark  structured streaming
        //create spark session    (local[*] means that  this driver will run the executers in parallel on this local machine. the number of executers is the  #cpu cores on the localhost)
        SparkSession spark = SparkSession.builder().appName("Rate Source").master("local[*]").getOrCreate();

        //Set Spark logging level to ERROR to avoid various other logs on console.
        spark.sparkContext().setLogLevel("ERROR");

        //create streaming dataframe
        //using rate input source    (fakes data: timestamp, value:0,1,2,3 ...) that creates 1 row per second
        Dataset<Row> initDF = spark.readStream()
                .format("rate")                     // read stream data from rate stream      https://jaceklaskowski.gitbooks.io/spark-structured-streaming/content/spark-sql-streaming-RateStreamSource.html
                .option("rowsPerSecond", 1)         //generate 1 row for each microbatch
                .load();                            //load the data into the initDF


        // Check if DataFrame is streaming or Not.
        System.out.println("Streaming DataFrame : " + initDF.isStreaming());    //check if the the dataset is straming or not


        //apply transformations over the input stream to create other streaming dataframe
        //use withColumn to add "result" column (result = 1 + column value)
        //other variations can be found at: https://medium.com/expedia-group-tech/start-your-journey-with-apache-spark-part-2-682891efda4b
        Dataset<Row> transformedDF = initDF
                .withColumn("result", col("value").plus(lit(1)));


        //use micro-batch (fakes ral time) to read the generated stream    and print it to te conso
        //using append mode
        //  Spark will output only newly processed rows since the last trigger. (eg on tI it will output data from Ti-1 to Ti )
        try
        {
            transformedDF                                        //take data from streaming DataFrame
                    .writeStream()                              //write the stream
                    .outputMode("append")                       //in append  mode !!!
                    .format("console")                          //to console
                    .option("truncate", false)
                    .start()
                    .awaitTermination();
        }
        catch (StreamingQueryException e) {
            e.printStackTrace();
        } finally {
            spark.close();
        }


    }
}
