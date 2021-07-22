package com.epam.naya_bdd_project.match_archivator

import com.epam.naya_bdd_project.common.models.Offer
import com.epam.naya_bdd_project.common.utils.DummyUtil_scala
import org.apache.spark.sql.{Encoders, SparkSession}
import com.epam.naya_bdd_project.common.models.{Match, User}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.StreamingQueryException


object Main_scala {

  private val userLog = Logger.getLogger("match_archivator_ms")


  def main(args: Array[String]): Unit = {

    //config
    //val config_mongo_db_uri = "mongodb://127.0.0.1/barter.";
    val config_hadoop_home_dir = "C:\\BigData\\hadoop-2.7.1"
    val config_spark_master = "local[*]";
    val config_kafka_bootstrap_servers = "localhost:9092";
    val config_kafka_checkpoint_root =  "c:\\todel15\\"; //"hdfs://hdfs:8020/checkpoints/"
    val config_kafka_fail_on_data_loss = "false"; //prod="true", dev="false"
    val confing_hdfs_root = "hdfs://localhost:8020/"


    //micro service
    val ms_source_topic = "newMatches";
    val ms_source_event_schema = Encoders.product[Match].schema
    val ms_target_hdfs_format = "json"; //"parquet"
    val ms_target_hdfs_file = "barter/matches";
    //val ms_sink_topic = "";


    //prevent exception of win utils
    System.setProperty("hadoop.home.dir", config_hadoop_home_dir)

    //Create Spark Session
    val spark = SparkSession.builder.appName("match archivator").master(config_spark_master).getOrCreate


    //create input stream from kafka
    val initDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config_kafka_bootstrap_servers)
      .option("subscribe", ms_source_topic)
      .option("failOnDataLoss",config_kafka_fail_on_data_loss)
      .load()
    initDF.printSchema()



    //transform stream to eventObjectsDF
    val eventsJsonDF = initDF.selectExpr("CAST(value AS STRING)")
    val eventObjectsDF = eventsJsonDF
      .select(from_json(col("value"), ms_source_event_schema).as("data"))
      .select("data.*")

    eventObjectsDF.printSchema()



    //write to hdfs
    try {
        eventObjectsDF
        .writeStream
        .outputMode("append")
        .format(ms_target_hdfs_format)
        .option("path", confing_hdfs_root + ms_target_hdfs_file)  //"hdfs://localhost:8020/user/hduser/todel"
        .option("checkpointLocation", config_kafka_checkpoint_root + "match_archivator")
        //.partitionBy("year", "month", "day")
        .start()
        .awaitTermination()
    }
    catch {
      case e: StreamingQueryException => e.printStackTrace()
    } finally {
      spark.close()
    }

  }

}
