package com.epam.naya_bdd_project.match_maker_ms
import com.epam.naya_bdd_project.common.models.Offer
import com.epam.naya_bdd_project.common.utils.DummyUtil_scala
import org.apache.spark.sql.{Encoders, SparkSession}


object Main_scala {
  def main(args: Array[String]): Unit = {

    //config
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


    //create streaming dataframe (latest)
    val initDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config_kafka_bootstrap_servers)
      .option("subscribe", ms_source_topic)
      //.option("failOnDataLoss","false")
      .load()
    initDF.printSchema()


  }

}
