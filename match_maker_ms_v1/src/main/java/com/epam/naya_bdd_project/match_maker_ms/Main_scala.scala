package com.epam.naya_bdd_project.match_maker_ms
import com.epam.naya_bdd_project.common.models.{Offer, User}
import com.epam.naya_bdd_project.common.utils.DummyUtil_scala
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.{Encoders, SparkSession}
import org.bson.Document


object Main_scala {
  def main(args: Array[String]): Unit = {

    //config
    val config_mongo_db_uri = "mongodb://127.0.0.1/barter.";
    val config_hadoop_home_dir = "C:\\BigData\\hadoop-2.7.1"
    val config_spark_master = "local[*]";
    val config_kafka_bootstrap_servers = "localhost:9092";

    //ms
    val ms_source_topic = "upsertedOffers";
    val ms_source_event_schema = Encoders.product[Offer].schema



    //prevent exception of win utils
    System.setProperty("hadoop.home.dir", config_hadoop_home_dir)

    //Create Spark Session
    val spark = SparkSession.builder.appName("Kafka Source").master(config_spark_master).getOrCreate


    //read from mongo users (usng pipline)
    val usersRDD1 = MongoSpark.load(spark.sparkContext, ReadConfig(Map("uri" -> (config_mongo_db_uri + "users"))))
    val usersSchema = Encoders.product[User].schema
    val usersRDD2 = usersRDD1.withPipeline(Seq(Document.parse("{ $match: { user_id : { $gt : 0 } } }")))
    val dbUsersDF  = usersRDD2.toDF(usersSchema)
    dbUsersDF.printSchema()
    dbUsersDF.show()


    //read from mongo offers (usng pipline)
    val offersRDD1 = MongoSpark.load(spark.sparkContext, ReadConfig(Map("uri" -> (config_mongo_db_uri + "offers"))))
    val offersSchema = Encoders.product[Offer].schema
    val offersRDD2 = offersRDD1.withPipeline(Seq(Document.parse("{ $match: { user_id : { $gt : 0 } } }")))
    val dbOffersDF  = offersRDD2.toDF(offersSchema)
    dbOffersDF.printSchema()
    dbOffersDF.show()



    //create input stream from kafka
    val initDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config_kafka_bootstrap_servers)
      .option("subscribe", ms_source_topic)
      //.option("failOnDataLoss","false")
      .load()
    initDF.printSchema()


    //transform stream to eventObjectsDF
    val eventsJsonDF = initDF.selectExpr("CAST(value AS STRING)")
    val eventObjectsDF = eventsJsonDF
      .select(from_json(col("value"), ms_source_event_schema).as("data"))
      .select("data.*")
      .withColumnRenamed("offer_guid", "stream_offer_guid")
      .withColumnRenamed("user_id", "stream_user_id")
      .withColumnRenamed("give", "stream_give")
      .withColumnRenamed("take", "stream_take")
    eventObjectsDF.printSchema()



    //transform stream to joinedDF
    eventObjectsDF.createOrReplaceTempView("eventsView")
    dbOffersDF.createOrReplaceTempView("dbOffersView")
    dbUsersDF.createOrReplaceTempView("dbUsersView")

    val join_query = "select * from eventsView e INNER JOIN dbOffersView o where rlike(stream_give,take) and rlike(give,stream_take)" //give and take come form db. stram_give,stream_take come from stream.
    val joinedDS = spark.sql(join_query)


    /*
    //test: write to console
    try {
      joinedDS
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", false)
        //.option("numRows", 3)
        .start()
        .awaitTermination()
    }
    catch {
      case e: StreamingQueryException => e.printStackTrace()
    } finally {
      spark.close()
    }
    */

  }

}
