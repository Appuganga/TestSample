package com.producer.kafka

import org.apache.log4j.Logger
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010._


object ConsumerApp {
  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger(ConsumerApp.getClass)
    PropertyHandler.loadProperties();
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "edge1.ideacelluar.com:9092,edge2.ideacellular.com:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))
    //val topics = Array("NOKIAMSC_10", "ERICOCCCS5_10", "ERICSMSCS5_10")
    val spark = SparkSession.builder.master(PropertyHandler.getValue("spark.master")).appName("Campaign App").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    //val sqlContext = new SQLContext(sc)
    //val kafkaParams = CampaignConsumerUtil.getKafkaParams();
    val streamingContext = new StreamingContext(sc, Seconds(2))
    var topic = PropertyHandler.getValue("kafka.topic")
    val stream = KafkaUtils.createDirectStream[String, String](streamingContext, PreferConsistent, Subscribe[String, String](topic.split(","), kafkaParams))

    //val stream = KafkaUtils.createDirectStream[String, Array[Byte]](streamingContext, PreferConsistent, Subscribe[String, Array[Byte]](topic.split(","), kafkaParams))

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //logger.info("Reading current offset Range..." + offsetRanges.mkString)
      try {
        rdd.map(record => {
          var isCommitOffset = false;
          val currentOffset = record.offset()
          //logger.info("Reading current offset value..." + currentOffset)

          try {
            //newToBankRecord = CampaignConsumerUtil.getRecord(record.value(), "")
            var csvValues = record.value()
              val put = AppUtil.prepareHbasePutRecord(csvValues)
              HbaseUtil.insertIntoHbase(PropertyHandler.getValue("hbase.table"), put)
            isCommitOffset = true;
          } catch {
            case e: Exception =>
              logger.error("Error Occurred while inserting values into DB ... Reason ..." + e.getStackTraceString + ", offset Values...:" + currentOffset)
              isCommitOffset = false
          }

          /**
           * map for offset->success|failure
           */

          (Array(OffsetRange(record.topic(), record.partition(), currentOffset, currentOffset + 1)), isCommitOffset);
        })
        //SQLHiveContextSingleton.closeHbaseConnection()
        //logger.info("Last accessed offset:  " + offsetRanges.mkString)
      } catch {
        case e: Exception => {
          logger.error("Error Occurred while reading Record ... Reason ..." + e.getStackTraceString + ", offset Values...:" + offsetRanges.mkString)
        }
      }

    }
  }
}