package com.epam.naya_bdd_project.match_maker_ms
import com.epam.naya_bdd_project.common.models.{Offer, User}
import com.epam.naya_bdd_project.common.utils.DummyUtil_scala
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.{Encoders, SparkSession}
import org.bson.Document


object Main_scala_read_bd_once_bad {
  def main(args: Array[String]): Unit = {

    //micro service
    val ms_source_topic = "upsertedOffers";
    val ms_source_event_schema = Encoders.product[Offer].schema
    val ms_sink_topic = "newMatches";


    //config
    val config_mongo_db_uri = "mongodb://127.0.0.1/barter.";
    val config_hadoop_home_dir = "C:\\BigData\\hadoop-2.7.1"
    val config_spark_master = "local[*]";
    val config_kafka_bootstrap_servers = "localhost:9092";
    val config_kafka_checkpoint_root =  "c:\\todel12\\"; //"hdfs://hdfs:8020/checkpoints/"
    val config_kafka_fail_on_data_loss = "false"; //prod="true", dev="false"



    //prevent exception of win utils
    System.setProperty("hadoop.home.dir", config_hadoop_home_dir)

    //Create Spark Session
    val spark = SparkSession.builder.appName("match maker").master(config_spark_master).getOrCreate


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
      .withColumnRenamed("offer_guid", "db_offer_guid")
      .withColumnRenamed("user_id", "db_user_id")
      .withColumnRenamed("give", "db_give")
      .withColumnRenamed("take", "db_take")

    dbOffersDF.printSchema()
    dbOffersDF.show()



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
      .withColumnRenamed("offer_guid", "stream_offer_guid")
      .withColumnRenamed("user_id", "stream_user_id")
      .withColumnRenamed("give", "stream_give")
      .withColumnRenamed("take", "stream_take")
    eventObjectsDF.printSchema()



    //transform stream to joined1DS:  rlike
    //stream_offer_guid|stream_user_id|stream_give|stream_take|db_offer_guid|db_user_id|db_give|db_take
    eventObjectsDF.createOrReplaceTempView("eventsView")
    dbOffersDF.createOrReplaceTempView("dbOffersView")
    dbUsersDF.createOrReplaceTempView("dbUsersView")
    //stream_user_id|stream_give|stream_take
    val join1_query = "select * from eventsView e INNER JOIN dbOffersView d where rlike(stream_give,db_take) and rlike(db_give,stream_take)"
    val joined1DS = spark.sql(join1_query)


    //transform stream to joined2DS:  add stream user details
    //|stream_offer_guid|stream_user_id|stream_give|stream_take||stream_email|stream_fname|stream_lname|db_offer_guid|db_user_id|db_give|db_take
    val joined2DS  =  joined1DS.join(dbUsersDF, joined1DS("stream_user_id") === dbUsersDF("user_id"), "leftouter")
      .drop("user_id")
      .withColumnRenamed("email", "stream_email")
      .withColumnRenamed("fname", "stream_fname")
      .withColumnRenamed("lname", "stream_lname")



    //transform stream to joined2DS:  add db user details
    val joined3DS  =  joined2DS.join(dbUsersDF, joined2DS("db_user_id") === dbUsersDF("user_id"), "leftouter")
      .drop("user_id")
      .withColumnRenamed("email", "db_email")
      .withColumnRenamed("fname", "db_fname")
      .withColumnRenamed("lname", "db_lname")


    /*
    //test: write to console
    try {
      joined3DS
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", false)
        .start()
        .awaitTermination()
    }
    catch {
          case e: StreamingQueryException => e.printStackTrace()
    } finally {
          spark.close()
    }
    */


    //write result to kafka
    val query = joined3DS.selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .outputMode("append")
      .option("kafka.bootstrap.servers", config_kafka_bootstrap_servers)
      .option("topic", ms_sink_topic)
      .option("checkpointLocation",  config_kafka_checkpoint_root + ms_sink_topic)
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

