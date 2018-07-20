package com.advanced.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object ByKeyExamples {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")
   
    val pairs = sc.parallelize(List(("prova", 3), ("ciao", 2),
                                ("prova", 2), ("ciao", 4),
                                ("prova", 1), ("ciao", 6))).cache()

    
    val res = pairs.aggregateByKey(List[Any]())(
      (aggr, value) => aggr ::: (value :: Nil),
      (aggr1, aggr2) => aggr1 ::: aggr2
    ).collect().toMap
    
    
    val res2 = pairs.combineByKey(
      (value) => List(value),
      (aggr: List[Any], value) => aggr ::: (value :: Nil),
      (aggr1: List[Any], aggr2: List[Any]) => aggr1 ::: aggr2
    ).collect().toMap
      
    // Print each result on its own line.
    println("---------aggregateByKey---------")
    res.foreach(println)
    
    println("----------combineByKey--------")
    res2.foreach(println)
    
    
    val grpResult = pairs.groupByKey();
    println("----------groupByKey--------")
    grpResult.foreach(println)
    
    
    val redRDD = pairs.reduceByKey((x,y) => x+y )
    println("----------reduceByKey--------")
    redRDD.foreach(println)
    
    val sortRDD = pairs.sortByKey(true, 0)
    println("----------sortByKey--------")
    sortRDD.foreach(println)
    
    
    val countRDD = pairs.countByValue();  // TODO
    println("----------countByValue--------")
    countRDD.foreach(println)
  }
}
