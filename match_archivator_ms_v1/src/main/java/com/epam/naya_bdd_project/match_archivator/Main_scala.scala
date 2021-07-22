package com.epam.naya_bdd_project.match_archivator

import com.epam.naya_bdd_project.common.models.Offer
import com.epam.naya_bdd_project.common.utils.DummyUtil_scala
import org.apache.spark.sql.{Encoders, SparkSession}
import com.epam.naya_bdd_project.common.models.{Match, User}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.StreamingQueryException


object Main_scala {

  private val userLog = Logger.getLogger(getClass().getName())


  def main(args: Array[String]): Unit = {

    //config
    //val config_mongo_db_uri = "mongodb://127.0.0.1/barter.";
    val configHadoopHomeDir = "C:\\BigData\\hadoop-2.7.1"
    val configSparkMaster = "local[*]";
    val configKafkaBootstrapServers = "localhost:9092";
    val configKafkaCheckpointRoot =  "c:\\todel17\\"; //"hdfs://hdfs:8020/checkpoints/"
    val configKafkaFailOnDataLoss = "false"; //prod="true", dev="false"
    val confingHdfsRoot = "hdfs://localhost:8020/"


    //micro service
    val msSourceTopic = "newMatches";
    val msSourceEventSchema = Encoders.product[Match].schema
    val msTargetHdfsFormat = "json"; //"parquet"
    val msTargetHdfsFile = "barter/matches";
    //val ms_sink_topic = "";


    //prevent exception of win utils
    System.setProperty("hadoop.home.dir", configHadoopHomeDir)

    //Create Spark Session
    val spark = SparkSession.builder.appName("match archivator").master(configSparkMaster).getOrCreate


    //create input stream from kafka
    val initDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", configKafkaBootstrapServers)
      .option("subscribe", msSourceTopic)
      .option("failOnDataLoss",configKafkaFailOnDataLoss)
      .load()
    initDF.printSchema()



    //transform stream to eventObjectsDF
    val eventsJsonDF = initDF.selectExpr("CAST(value AS STRING)")
    val eventObjectsDF = eventsJsonDF
      .select(from_json(col("value"), msSourceEventSchema).as("data"))
      .select("data.*")

    eventObjectsDF.printSchema()



    //write to hdfs
    try {
        eventObjectsDF
        .writeStream
        .outputMode("append")
        .format(msTargetHdfsFormat)
        .option("path", confingHdfsRoot + msTargetHdfsFile)  //"hdfs://localhost:8020/user/hduser/todel"
        .option("checkpointLocation", configKafkaCheckpointRoot + "matchArchivator")
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
