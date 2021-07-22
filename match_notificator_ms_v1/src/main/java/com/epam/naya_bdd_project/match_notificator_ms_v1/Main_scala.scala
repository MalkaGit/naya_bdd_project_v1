package com.epam.naya_bdd_project.match_notificator_ms_v1

import com.epam.naya_bdd_project.common.models.Match
import com.epam.naya_bdd_project.common.utils.DummyUtil_scala
import com.epam.naya_bdd_project.match_notificator_ms_v1.utils.MailSenderUtil
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.StreamingQueryException
import org.bson.Document

object Main_scala {

  private val userLog = Logger.getLogger("match_notificator_ms")


  //config
  //private val config_mongo_db_uri = "mongodb://127.0.0.1/barter.";
  private val configHadoopHomeDir = "C:\\BigData\\hadoop-2.7.1"
  private val configSparkMaster = "local[*]";
  private val configKafkaBootstrapServers = "localhost:9092";
  private val configKafkaCheckpointRoot =  "c:\\todel16\\"; //"hdfs://hdfs:8020/checkpoints/"
  private val configKafkaFailOnDataLoss = "false"; //prod="true", dev="false"
  //private val confingHdfsRoot = "hdfs://localhost:8020/"


  //micro service
  private val msSourceTopic = "newMatches";
  private val msSourceEventSchema = Encoders.product[Match].schema
  private val msTargetSenderEmail = "regine.issan@gmail.com";
  private val msTargetSenderPssword = "Pumiki14";



  def processMicroBatch(batchDF: DataFrame, batchId: Long) :Unit = {


    userLog.info("matc notificator: process micro batch was called with " + batchDF.count() + " rows");

    //streamDF
    val eventObjectsDF = batchDF;
    eventObjectsDF.printSchema()

    eventObjectsDF.foreach(r=> {
      val streamFname= r.getAs[String]("streamFname")
      val streamLname = r.getAs[String]("streamLname")
      val streamEmail = r.getAs[String]("streamEmail")
      val streamGive= r.getAs[String]("streamGive")
      val streamTake= r.getAs[String]("streamTake")
      val dbFname= r.getAs[String]("dbFname")
      val dbLname= r.getAs[String]("dbLname")
      val dbEmail= r.getAs[String]("dbEmail")
      val dbGive = r.getAs[String]("dbGive")
      val dbTake= r.getAs[String]("dbTake")



      //send mail to the stream user
      val textMessageToStreamUser = String.format("Hello %s %s, the offer you just proposed was matched with %s %s ! you proposed %s for %s. Your match propose %s for %s", streamFname, streamLname, dbFname, dbLname, streamGive, streamTake, dbGive, dbTake)
      MailSenderUtil.send(streamEmail,msTargetSenderEmail,msTargetSenderPssword,"New match",textMessageToStreamUser)

      //send mail to the fb user
      val textMessageToDbUser = String.format("Hello %s %s,  %s %s matched an offer that matches your offer ! you proposed %s for %s. Your match propose %s for %s", dbFname, dbLname, streamFname, streamLname, dbGive, dbTake, streamGive, streamTake)
      MailSenderUtil.send(dbEmail,msTargetSenderEmail,msTargetSenderPssword,"New match",textMessageToDbUser)

      userLog.info(("sent email notification to " + streamEmail + " and to " + dbEmail))
      //todo: error hadling - write to errors queue

    });


  }

  def main(args: Array[String]): Unit = {



    //prevent exception of win utils
    System.setProperty("hadoop.home.dir", configHadoopHomeDir)

    //Create Spark Session
    val spark = SparkSession.builder.appName("match archivator").master(configSparkMaster).getOrCreate


    //create input stream from kafka
    val initDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", configKafkaBootstrapServers)
      .option("subscribe", msSourceTopic)
      .option("failOnDataLoss",configKafkaFailOnDataLoss )
      .load()
    initDF.printSchema()



    //transform stream to eventObjectsDF
    val eventsJsonDF = initDF.selectExpr("CAST(value AS STRING)")
    val eventObjectsDF = eventsJsonDF
      .select(from_json(col("value"), msSourceEventSchema).as("data"))
      .select("data.*")

    eventObjectsDF.printSchema()


    //use  foreachBatch to join the batch events and write to kaka
    val query = eventObjectsDF
      //.selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .outputMode("append")
      //.trigger(Trigger.ProcessingTime("10 seconds"))
      .foreachBatch((batchDF, batchId)=>processMicroBatch(batchDF, batchId))
      .start();


    try {
      query.awaitTermination();
    }
    catch {
      case e: StreamingQueryException => e.printStackTrace()
    } finally {
      spark.close();
    }

  }
}




