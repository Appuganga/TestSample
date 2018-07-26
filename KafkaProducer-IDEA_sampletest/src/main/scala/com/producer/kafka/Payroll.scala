package com.producer.kafka

import scala.tools.scalap.Main
import scala.io.Source
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.rdd.RDD

object Payroll {

  case class Employee(Fiscal_Year: String, Agency: String, Last_Name: String, First_Name: String, Mid_Init: String, Agency_Start_Date: String,
                      Work_Location_Borough: String, Title_Description: String, Leave_Status_as_of_June_30: String, Base_Salary: String, Pay_Basis: String,
                      Regular_Hours: String, Regular_Gross_Paid: String, OT_Hours: String, Total_OT_Paid: String, Total_Other_Pay: String)
  //case class Employee4 (Fiscal_Year:String, Agency:String,Last_Name:String )

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Sample").setMaster("local[2]")

    val spark = org.apache.spark.sql.SparkSession.builder
      .config(conf)
      .getOrCreate;

    val rdd = spark.sparkContext.textFile("C:/Users/KOGENTIX/Downloads/Citywide_Payroll_Data__Fiscal_Year_.csv")

    def getEmployeeRDD(inputrdd: RDD[String]): RDD[Employee] = {
      val schemardd = inputrdd.map(x => x.split(",", 16)).map(x => Employee(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13), x(14), x(15)))
      schemardd
    }

    val schemardd = getEmployeeRDD(rdd)

    def getHighestAvgSalPerLoc(payBasis: String, employee: RDD[Employee]): RDD[(String, String)] = {

      //val resrdd = employee.filter(rec => rec.Pay_Basis == payBasis)
      //      res2.collect().foreach(println)

      val res2 = employee.groupBy(x => x.Work_Location_Borough)
        .map(row => {
          val key = row._1

          val maxSal = row._2.map(emp => {
            (emp.Base_Salary)
          }).max
          (key, maxSal)
        })

      res2

      /* val twoJoin=checks.join(test,checks.col("Work_Location_Borough").equalTo(test.col("Work_Location_Borough_New")),"left_outer")
							.groupBy(test.col("Work_Location_Borough_New"),test.col("Pay_Basis")).agg(sum("Base_Salary_new")).show()*/
      //employee.

    }

    val highestAvgsal = getHighestAvgSalPerLoc("per Hour", schemardd)

    highestAvgsal.collect().foreach(println)

    /*	import spark.implicits._

							val checks=schemardd.toDF()
							val test=checks.withColumn("Base_Salary_new",regexp_replace(checks("Base_Salary"),"\\$"," "))
							.withColumn("Work_Location_Borough_New",checks.col("Work_Location_Borough"))
							print("with column")




							val twoJoin=checks.join(test,checks.col("Work_Location_Borough").equalTo(test.col("Work_Location_Borough_New")),"left_outer")
							.groupBy(test.col("Work_Location_Borough_New"),test.col("Pay_Basis")).agg(sum("Base_Salary_new")).show()
							*/

    // print("before groupby")
    //	val resultCheck=checks.select("Base_Salary","Work_Location_Borough","Pay_Basis").groupBy("Work_Location_Borough").sum("Base_Salary").show()

  }

}