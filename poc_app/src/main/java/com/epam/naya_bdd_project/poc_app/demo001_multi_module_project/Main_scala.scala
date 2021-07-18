package com.epam.naya_bdd_project.poc_app.demo001_multi_module_project

import com.epam.naya_bdd_project.common.models.{Offer, User}
import com.epam.naya_bdd_project.common.utils.DummyUtil_scala
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.bson.Document
import com.mongodb.spark.config.ReadConfig

object Main_scala {
  def main(args: Array[String]): Unit = {

    //ms consts
    val ms_source_topic = "upsertedOffers";
    val ms_source_schema  = Encoders.product[Offer].schema
    val ms_sink_topic = "newMatches";



    //config
    val config_mongo_db_uri = "mongodb://127.0.0.1/barter.";
    val config_kafka_bootstrap_servers = "localhost:9092";  //the kafka server
    val config_kafka_checkpoint_location_root = "c:\\todel12\\";  //hdfs://hdfs:8020/checkpoints
    val config_hadoop_home_dir = "C:\\BigData\\hadoop-2.7.1";



    //prevent exeption: Could not locate executable null\bin\winutils.exe in the Hadoop binaries
    System.setProperty("hadoop.home.dir", config_hadoop_home_dir)

    //build spark session
    val spark = SparkSession.builder.appName("match maker").master("local[*]").getOrCreate


    //read mongoUsersDF (using MongoSpark)
    val mongoUsersRDD1 = MongoSpark.load(spark.sparkContext, ReadConfig(Map("uri" -> (config_mongo_db_uri + "users"))))
    val mongoUsersRDD2 = mongoUsersRDD1.withPipeline(Seq(Document.parse("{ $match: { user_id : { $gt : 0 } } }")))
    val mongoUsersSchema = Encoders.product[User].schema
    val mongoUsersDF     = mongoUsersRDD2.toDF(mongoUsersSchema)
    mongoUsersDF.printSchema()
    mongoUsersDF.show()


    //read mongoOffersDF (using MongoSpark)
    val mongoOffersRDD1 = MongoSpark.load(spark.sparkContext, ReadConfig(Map("uri" -> (config_mongo_db_uri + "offers"))))
    val mongoOffersRDD2 = mongoOffersRDD1.withPipeline(Seq(Document.parse("{ $match: { user_id : { $gt : 0 } } }")))
    val mongoOffersSchema = Encoders.product[Offer].schema
    val mongoOffersDF     = mongoOffersRDD2.toDF(mongoOffersSchema)
    mongoOffersDF.printSchema()
    mongoOffersDF.show()



    //read (latest) events from kafka ms_source_topic
    val initDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config_kafka_bootstrap_servers)
      .option("subscribe", ms_source_topic)
      //.option("failOnDataLoss", "false")
      .load()
    initDF.printSchema()


    //create event objects DF from the init DF
    val eventsJsonDF = initDF.selectExpr("CAST(value AS STRING)")
    val eventObjectsDF = eventsJsonDF
      .select(from_json(col("value"), ms_source_schema).as("data"))
      .select("data.*")
      .withColumnRenamed("offer_guid", "stream_offer_guid")
      .withColumnRenamed("user_id", "stream_user_id")
      .withColumnRenamed("give", "stream_give")
      .withColumnRenamed("take", "stream_take")

    eventObjectsDF.printSchema()

    /*
    //test: comment the rest of the code. use this block to read from input topic
    try {
      eventsJsonDF
        .writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", false)
        .option("numRows", 3)
        .start()
        .awaitTermination()
    }
    catch {
      case e: StreamingQueryException => e.printStackTrace()
    } finally {
      spark.close()
    }
    */


    //join
    mongoUsersDF.createOrReplaceTempView("dbUsersView")
    mongoOffersDF.createOrReplaceTempView("dbOffersView")
    eventObjectsDF.createOrReplaceTempView("eventObjectsView")
    val join_query = "select * from eventObjectsView e INNER JOIN dbOffersView d where rlike(stream_give,take) and rlike(give,stream_take)"  //give,take - from db, stream_give and stream_take from stream
    val joinedDS = spark.sql(join_query)


    /*
   //test: comment the rest of the code. use this block to read from input topic
   try {
     joinedDS
       .writeStream
       .outputMode("update")
       .format("console")
       .option("truncate", false)
       .option("numRows", 10)
       .start()
       .awaitTermination()
   }
   catch {
     case e: StreamingQueryException => e.printStackTrace()
   } finally {
     spark.close()
   }
  */



  //write to kafka
    val query =  joinedDS.selectExpr( "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .outputMode("append")
      .option("kafka.bootstrap.servers", config_kafka_bootstrap_servers)
      .option("topic", ms_sink_topic)
      .option("checkpointLocation", config_kafka_checkpoint_location_root+ ms_sink_topic)
      .start();
    try {
      query.awaitTermination();
    }
    catch {
      case e: StreamingQueryException =>  e.printStackTrace()
    } finally {
      spark.close();
    }

  }

}
