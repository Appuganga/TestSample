package com.producer.kafka

import org.apache.spark.SparkConf

object BothCheck {
  def  main(args: Array[String]): Unit = {
		
			val conf = new SparkConf().setAppName("Sample").setMaster("local[2]")

					val spark = org.apache.spark.sql.SparkSession.builder
        .config(conf)
        .getOrCreate;
					

			//sourcerdd.map( row=> getCountry(row))
			val sourcerdd =spark.sparkContext.textFile("C:/Users/KOGENTIX/Desktop/Data1.txt")
			
			def sample(sourcerdd:String)={
			//sourcerdd.split()
			  
			}
			  
			
	
}

}