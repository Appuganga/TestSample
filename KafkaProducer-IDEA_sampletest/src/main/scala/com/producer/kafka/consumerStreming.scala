package com.producer.kafka

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.CanCommitOffsets
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.hadoop.hbase.rest.client.RemoteHTable

object consumerStreming {

  val logger = Logger.getLogger(consumerStreming.getClass)

  def main(args: Array[String]): Unit = {

    PropertyHandler.loadProperties();
    val kafkaParams = Map[String, Object](
      /*      "bootstrap.servers" -> "edge1.ideacelluar.com:9092,edge2.ideacellular.com:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)*/ 
      "bootstrap.servers" -> PropertyHandler.getValue("bootstrap.servers"),
      "group.id" -> PropertyHandler.getValue("group.id"),
      "enable.auto.commit" -> PropertyHandler.getValue("enable.auto.commit"),
      "security.protocol" -> PropertyHandler.getValue("security.protocol"),
      "sasl.kerberos.service.name" -> PropertyHandler.getValue("sasl.kerberos.service.name"),
      //"auto.commit.interval.ms" -> "1000",
      "session.timeout.ms" -> PropertyHandler.getValue("session.timeout.ms"),
      "key.deserializer" -> PropertyHandler.getValue("key.deserializer"),
      "value.deserializer" -> PropertyHandler.getValue("value.deserializer"),
      "auto.offset.reset" -> PropertyHandler.getValue("auto.offset.reset"),
      "max.poll.records" -> PropertyHandler.getValue("max.poll.records"))
    val spark = SparkSession.builder.master(PropertyHandler.getValue("spark.master")).appName("Idea_Hbase_Consumer_App").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    //val sqlContext = new SQLContext(sc)
    //val kafkaParams = CampaignConsumerUtil.getKafkaParams();
    val streamingContext = new StreamingContext(sc, Seconds(2)) //time based window
    var topic = PropertyHandler.getValue("kafka.topic")
    val stream = KafkaUtils.createDirectStream[String, String](streamingContext, PreferConsistent, Subscribe[String, String](topic.split(","), kafkaParams))

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //logger.info("Reading current offset Range..." + offsetRanges.mkString)
      try {
        rdd.map(record => {

          PropertyHandler.loadProperties();

          var isCommitOffset = false;
          val currentOffset = record.offset()
          logger.info("Reading current offset value..." + currentOffset)

          try {
            var csvValues = record.value()
            logger.info("current offset value..." + csvValues)
            val put = AppUtil.prepareHbasePutRecord(csvValues)
            
             
            var client = com.producer.kafka.HbaseUtil.getHbaseRestCOnnection();
            logger.info("entered hbaseconnection")
              val targetTable = new RemoteHTable(client, PropertyHandler.getValue("hbase.table"))
              if (targetTable != null) {
                logger.info("entered if loop")
                targetTable.put(put)
                logger.info("put the record")
                isCommitOffset = true;
              } else {
                isCommitOffset = false;
                logger.info("Failed t insert values in DB ... , offset Values...:" + currentOffset)
              }
      
            //hTableMap + (config.stepid -> targetTable)
            //HbaseUtil.insertIntoHbase(PropertyHandler.getValue("hbase.table"), put)

          } catch {
            case e: Exception =>
              logger.error("Error Occurred while inserting values into DB ... Reason ..." + e.getMessage + "," + e.getStackTrace.toString() + ", offset Values...:" + currentOffset)
              isCommitOffset = false
          }

          /**
           * map for offset->success|failure
           */

          (Array(OffsetRange(record.topic(), record.partition(), currentOffset, currentOffset + 1)), isCommitOffset);
        }).collectAsMap().map(f => {
          if (f._2) {
            stream.asInstanceOf[CanCommitOffsets].commitAsync(f._1)
            logger.info("Successfull offset:  " + f._1.mkString)
          } else {
            logger.info("Failed offset:  " + f._1.mkString)

          }
        })
        //HbaseUtil.closeHbaseConnection()
        logger.info("Last accessed offset:  " + offsetRanges.mkString)
      } catch {
        case e: Exception => {
          logger.error("Error Occurred while reading Record ... Reason ..." + e.getStackTraceString + ", offset Values...:" + offsetRanges.mkString)
        }
      }

    }
    streamingContext.start()
    streamingContext.awaitTermination()

  }
}