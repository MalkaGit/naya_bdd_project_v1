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
  private val config_hadoop_home_dir = "C:\\BigData\\hadoop-2.7.1"
  private val config_spark_master = "local[*]";
  private val config_kafka_bootstrap_servers = "localhost:9092";
  private val config_kafka_checkpoint_root =  "c:\\todel16\\"; //"hdfs://hdfs:8020/checkpoints/"
  private val config_kafka_fail_on_data_loss = "false"; //prod="true", dev="false"
  //private val confing_hdfs_root = "hdfs://localhost:8020/"


  //micro service
  private val ms_source_topic = "newMatches";
  private val ms_source_event_schema = Encoders.product[Match].schema
  private val ms_target_sender_email = "regine.issan@gmail.com";
  private val ms_target_sender_pssword = "Pumiki14";



  //MailSenderUtil.send("regine.issan.jobs@gmail.com","regine.issan@gmail.com","Pumiki14","s1","b1");
  def processMicroBatch(batchDF: DataFrame, batchId: Long) :Unit = {


    userLog.info("matc notificator: process micro batch was called with " + batchDF.count() + "rows");

    //streamDF
    val eventObjectsDF = batchDF;
    eventObjectsDF.printSchema()

    eventObjectsDF.foreach(r=> {
      val stream_fname= r.getAs[String]("stream_fname")
      val stream_lname = r.getAs[String]("stream_lname")
      val stream_email = r.getAs[String]("stream_email")
      val stream_give= r.getAs[String]("stream_give")
      val stream_take= r.getAs[String]("stream_take")
      val db_fname= r.getAs[String]("db_fname")
      val db_lname= r.getAs[String]("db_lname")
      val db_email= r.getAs[String]("db_email")
      val db_give = r.getAs[String]("db_give")
      val db_take= r.getAs[String]("db_take")



      //send mail to the stream user
      val text_message_to_stram_user = String.format("Hello %s %s, /r/n the offer you just proposed was matched with %s %s ! you proposed %s for %s. Your match propose %s for %s", stream_fname, stream_lname, db_fname, db_lname, stream_give, stream_take, db_give, db_take)
      MailSenderUtil.send(stream_email,ms_target_sender_email,ms_target_sender_pssword,"New match",text_message_to_stram_user)

      //send mail to the fb user
      val text_message_to_db_user = String.format("Hello %s %s, /r/n your offer was matched with %s %s ! you proposed %s for %s. Your match propose %s for %s", db_fname, db_lname, stream_fname, stream_lname, db_give, db_take, stream_give, stream_take)
      MailSenderUtil.send(db_email,ms_target_sender_email,ms_target_sender_pssword,"New match",text_message_to_db_user)

      userLog.info(("sent email notification to " + stream_email))
      //todo: error hadling - write to errors queue

    });


  }

  def main(args: Array[String]): Unit = {



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




