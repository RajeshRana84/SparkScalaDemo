package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object PurchaseByCustomer {
  
  
    def parseLine(line: String) = {
      // Split by commas
      val fields = line.split(",")
      // Extract the age and numFriends fields, and convert to integers
      val custId = fields(0).toInt
      val amount = fields(2).toFloat
      // Create a tuple that is our result.
      (custId, amount)
  }
  
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PurchaseByCustomer")   
    
    // Read each line of csv into an RDD
    val input = sc.textFile("../customer-orders.csv")
    
    val rdd = input.map(parseLine).reduceByKey( (x,y) => x + y )
    
    val newRdd = rdd.map(x => (x._2 , x._1)).sortByKey()    
    
    val sortedData = newRdd.collect();
    
     for (result <- sortedData) {
      val amount = result._1
      val id = result._2
      println(s"$id: $amount")
    }
    
  
  }
  
  
}