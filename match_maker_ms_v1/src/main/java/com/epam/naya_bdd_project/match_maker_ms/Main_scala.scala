package com.epam.naya_bdd_project.match_maker_ms
import com.epam.naya_bdd_project.common.models.{Offer, User}
import com.epam.naya_bdd_project.common.utils.DummyUtil_scala
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.bson.Document


object Main_scala {

  private val userLog = Logger.getLogger("match_maker_ms")

  //micro service
  private val msSourceTopic = "upsertedOffers";
  private val msSourceEventSchema = Encoders.product[Offer].schema
  private val msSinkTopic = "newMatches";

  //config
  private val configMongoDbUri = "mongodb://127.0.0.1/barter.";
  private val configHadoopHomeDir = "C:\\BigData\\hadoop-2.7.1"
  private val configSparkMaster = "local[*]";
  private val configKafkaBootstrapServers = "localhost:9092";
  private val configKafkaCheckpointRoot =  "c:\\todel12\\"; //"hdfs://hdfs:8020/checkpoints/"
  private val configKafkaFailOnDataLoss = "false"; //prod="true", dev="false"

  //Create Spark Session
  val spark = SparkSession.builder.appName("match maker").master(configSparkMaster).getOrCreate



  def processMicroBatch(batchDF: DataFrame, batchId: Long) :Unit = {

    userLog.info("match maker: process micro batch was called with " + batchDF.count() + " rows");


    //read from mongo users (usng pipline)
    //todo: instead reading all, read only those that were updates since last micto batch  (alos linit one minth)
    val usersRDD1 = MongoSpark.load(spark.sparkContext, ReadConfig(Map("uri" -> (configMongoDbUri + "users"))))
    val usersSchema = Encoders.product[User].schema
    val usersRDD2 = usersRDD1.withPipeline(Seq(Document.parse("{ $match: { userId : { $gt : 0 } } }")))
    val dbUsersDF  = usersRDD2.toDF(usersSchema)
    dbUsersDF.printSchema()
    dbUsersDF.show()


    //read from mongo offers (usng pipline)
    //todo: instead reading all, read only those that were updates since last micto batch  (alos linit one minth)
    val offersRDD1 = MongoSpark.load(spark.sparkContext, ReadConfig(Map("uri" -> (configMongoDbUri + "offers"))))
    val offersSchema = Encoders.product[Offer].schema
    val offersRDD2 = offersRDD1.withPipeline(Seq(Document.parse("{ $match: { userId : { $gt : 0 } } }")))
    val dbOffersDF  = offersRDD2.toDF(offersSchema)
      .withColumnRenamed("offerGuid", "dbOfferGuid")
      .withColumnRenamed("userId", "dbUserId")
      .withColumnRenamed("give", "dbGive")
      .withColumnRenamed("take", "dbTake")
    dbOffersDF.printSchema()
    dbOffersDF.show()



    //streamDF
    val eventObjectsDF = batchDF;
    eventObjectsDF.printSchema()


    //transform stream to joined1DS:  rlike
    //stream_offer_guid|stream_user_id|stream_give|stream_take|db_offer_guid|db_user_id|db_give|db_take
    //note: we cant use createOrReplaceTempView from foreachBatch. instead use createOrReplaceGlobalTempView and global_tmp
    //      https://stackoverflow.com/questions/62709024/temporary-view-in-spark-structure-streaming
    eventObjectsDF.createOrReplaceGlobalTempView ("eventsView")
    dbOffersDF.createOrReplaceGlobalTempView ("dbOffersView")
    dbUsersDF.createOrReplaceGlobalTempView ("dbUsersView")


    //stream_user_id|stream_give|stream_take
    val join1_query = "select * from global_temp.eventsView e INNER JOIN global_temp.dbOffersView d where rlike(streamGive,dbTake) and rlike(dbGive,streamTake)"
    val joined1DS = spark.sql(join1_query)


    //transform stream to joined2DS:  add stream user details
    //|stream_offer_guid|stream_user_id|stream_give|stream_take||stream_email|stream_fname|stream_lname|db_offer_guid|db_user_id|db_give|db_take
    val joined2DS  =  joined1DS.join(dbUsersDF, joined1DS("streamUserId") === dbUsersDF("userId"), "leftouter")
      .drop("userId")
      .withColumnRenamed("email", "streamEmail")
      .withColumnRenamed("fname", "streamFname")
      .withColumnRenamed("lname", "streamLname")



    //transform stream to joined2DS:  add db user details
    val joined3DS  =  joined2DS.join(dbUsersDF, joined2DS("dbUserId") === dbUsersDF("userId"), "leftouter")
      .drop("user_id")
      .withColumnRenamed("email", "dbEmail")
      .withColumnRenamed("fname", "dbFname")
      .withColumnRenamed("lname", "dbLname")


    //wrtte the join result to kafka (creates a small file)
    joined3DS
      .selectExpr("to_json(struct(*)) AS value")
      .write
      .format("kafka") //write stream to kafka (topic)
      //.outputMode("append")
      .option("kafka.bootstrap.servers", configKafkaBootstrapServers)
      .option("topic", msSinkTopic) //the topic we want to write to
      .option("checkpointLocation",  configKafkaCheckpointRoot + msSinkTopic)
      .mode("append")
      .save()
  }



  def main(args: Array[String]): Unit = {

    //prevent exception of win utils
    System.setProperty("hadoop.home.dir", configHadoopHomeDir)

    //create input stream from kafka
    val initDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", configKafkaBootstrapServers)
      .option("subscribe", msSourceTopic)
      .option("failOnDataLoss",configKafkaFailOnDataLoss)
      .load()
    initDF.printSchema()

    //convert the binary (kafka) value to String using selectExpr
    val eventsJsonDF = initDF.selectExpr("CAST(value AS STRING)")


    //create objects DF from the json DF
    val eventObjectsDF = eventsJsonDF
      .select(from_json(col("value"), msSourceEventSchema).as("data"))
      .select("data.*")
      .withColumnRenamed("offerGuid", "streamOfferGuid")
      .withColumnRenamed("userId", "streamUserId")
      .withColumnRenamed("give", "streamGive")
      .withColumnRenamed("take", "streamTake")
    eventObjectsDF.printSchema()


    //use  foreachBatch to join with latest db data and write to kaka
    val query = eventObjectsDF
      .writeStream
      .outputMode("append")
      //.trigger(Trigger.ProcessingTime("10 seconds")) //optional: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers
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
