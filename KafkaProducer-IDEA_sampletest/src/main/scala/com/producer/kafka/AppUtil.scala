package com.producer.kafka

import org.apache.hadoop.hbase.client.Put

import org.apache.log4j._
import org.apache.hadoop.hbase.util.Bytes

object AppUtil {
  val logger = Logger.getLogger(AppUtil.getClass)
  def prepareHbasePutRecord(csvValues: String): Put = {
    var put: Put = null;
    try {
      var str = PropertyHandler.getValue("columns.details")
      var rowkeys = PropertyHandler.getValue("rowkey.positions")
      val columnFamily = PropertyHandler.getValue("column.family")
      val columns: Array[String] = str.split("#")
      var count = 0;
      val values: Array[String] = csvValues.split(",")
      
      val rowKeyPositions:Array[String]=rowkeys.split(",");
      
      logger.info("split csv values.." + values.mkString(","))
      logger.info("split column values.." + columns.mkString(","))

     // put = new Put(Bytes.toBytes(String.valueOf(values(0) + "_" + values(10) + "_" + values(11)).trim()))
       put = new Put(Bytes.toBytes(String.valueOf(values(rowKeyPositions(0).toInt) + "-" + values(rowKeyPositions(1).toInt) + "-" + values(rowKeyPositions(2).toInt))))
      
      var value = ""
      for (column: String <- columns) {
        if (count < values.length) {
          value = values(count);
          value = if (value == null) "" else value
          count = count + 1;
        }else{
          value =""
        }
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(String.valueOf(value).trim()));
      }
    } catch {
      case e: Exception => logger.error("Error occured in preparing PUT record.." + e.getMessage + "," + e.getStackTraceString)
    }
    put
  }

}