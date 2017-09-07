package org.test.spark

import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;



object wordcount {
  
  def main(args:Array[String])=
  {
    println("Hello Wordl")
    
   // val conf= new SparkConf()
   //     .setAppName("WordCount")
   //     .setMaster("local")
   // val sc = new SparkContext(conf)
    System.setProperty("hadoop.home.dir","c:\\winutils");
    
    
    val sc = new SparkContext("local","wordcount",new SparkConf())
    
    val rdd1= sc.textFile("wordcount.txt")
    val rdd2=rdd1.flatMap(line=> line.split("\t"))
    val rdd3=rdd2.map(word=>(word,1)).reduceByKey(_+_)
              .saveAsTextFile("wordOutPut")
    //System.setProperty("hadoop.home.dir", "C:\winutils");
              sc.stop()
  }
}