package com.producer.kafka

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.storage.StorageLevel

object Sample {
 
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Sample").setMaster("local[2]")
    
     val sc= new SparkContext(conf)
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._
    println ("Hello Scala")
   
    
    
    
   def readExcel(file: String): DataFrame  = sqlContext.read
    .format("com.crealytics.spark.excel")
    .option("sheetName", "test")
    .option("location", file)
    .option("useHeader", "true")
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "true")
    .option("addColorColumns", "False")
    .load()
    

val data = readExcel("C:/Users/KOGENTIX/Desktop/track.xlsx")
data.show(false)
data.persist(StorageLevel.MEMORY_AND_DISK)

val outPut= data.repartition(1).write
        .format("com.databricks.spark.csv")
        .option("sheetName", "test")
        .option("header", "true")
        .mode("overwrite")
        .save("C:/Users/KOGENTIX/Desktop/data")


  


    
  


    
    
  }
}