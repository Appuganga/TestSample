package com.producer.kafka

import java.io.FileNotFoundException
import java.util.Properties

import scala.io.Source

import org.apache.log4j.Logger

/**
 * @author Shiva Balena
 */
object PropertyHandler extends Serializable {
  val logger = Logger.getLogger(PropertyHandler.getClass)
  var props: Properties = null;
  def loadProperties() = {
    try {
      if (props == null) {
        props = new Properties()
        val source = Source.fromFile("application.properties").bufferedReader()
        props.load(source)
      }
    } catch {
      case e: FileNotFoundException => logger.error("Error Occured while loading properties file...Reason :" + e.getMessage)
    }
  }

  def getValue(key: String): String = {
    if (props != null) {
      val value = props.getProperty(key)
      logger.info("Key :" + key + ", Value:" + value)
      return value

    }
    return null;
  }

}