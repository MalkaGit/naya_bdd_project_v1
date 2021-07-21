/*
package com.empa.d_course_tasks.task110d_read_mongo_foreah_micro_batch_anod_join_with_stream

import com.empa.d_course_tasks.task110c_DOC_mongoSpark_cnosole_writes_events_to_topic2__spark_streaming_reads_topic2__join_with_mongo_spark_filter___write_to_topic3.model.User
import com.empa.d_course_tasks.task110d_read_mongo_foreah_micro_batch_anod_join_with_stream.model.OfferEvent
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.StreamingQueryException
import org.bson.Document

object Main_scala_v2 {

  private val spark = SparkSession.builder.appName("Kafka Source").master("local[*]").getOrCreate
  private val mongoDataframe = MongoSpark.load(spark.sparkContext, ReadConfig(
    Map("uri" -> "mongodb://127.0.0.1/test.users")
  ))


  def processMicroBatch(batchDF: DataFrame, batchId: Long) :Unit = {

    val log = Logger.getLogger(classOf[User])

    log.info("Start Spark application")


    //read mongo
    val mongoSchema = Encoders.product[User].schema
    val momgoRdd = mongoDataframe.withPipeline(Seq(Document.parse("{ $match: { user_id : { $gt : 0 } } }")))
    val mongoDF  = momgoRdd.toDF(mongoSchema)
    mongoDF.printSchema()
    mongoDF.show()


    val eventObjectsDF = batchDF;



    //option1 - using expression
    //note: sung Seq we  do not get user_id twice in result
    //      solution: https://kb.databricks.com/data/join-two-dataframes-duplicated-columns.html
    val joinedDS  =  eventObjectsDF.join(mongoDF, Seq("user_id"), "leftouter")




    //batchDF
    joinedDS
      .selectExpr("to_json(struct(*)) AS value")
      .write
      .format("kafka") //write stream to kafka (topic)
      //.outputMode("append")
      .option("kafka.bootstrap.servers", "localhost:9092") //the details of the kafka server
      .option("topic", "test3topic") //the topic we want to write to
      .option("checkpointLocation", "c:\\todel12\\test3topic")
      .mode("append")
      .save()


  }



  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\BigData\\hadoop-2.7.1")

    val failOnDataLoss = "false" //false for dec
    spark. sparkContext.setLogLevel("ERROR")


    //create streaming dataframe
    val initDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test2topic")
      .option("failOnDataLoss", "false")
      .load()

    initDF.printSchema()


    //convert the binary (kafka) value to String using selectExpr
    val eventsJsonDF = initDF.selectExpr("CAST(value AS STRING)")



    //create objects DF from the json DF
    val eventSchema = Encoders.product[OfferEvent].schema
    val eventObjectsDF = eventsJsonDF
      .select(from_json(col("value"), eventSchema).as("data"))
      .select("data.*")
    eventObjectsDF.printSchema()




    //with foreach batch
    val query = eventObjectsDF
      .selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .foreachBatch((batchDF, batchId)=>Main_scala_v1.processMicroBatch(batchDF, batchId))
      .outputMode("append")
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
*/