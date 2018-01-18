package com.bjsxt.scala.spark.operator

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object DistinctOperator {
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setAppName("DistinctOperator").setMaster("local")
    val sc = new SparkContext(conf)
     
     
     val list = List(
       ("bjsxt",1),
       ("bjsxt",2),
       ("shsxt",1)
     )
     val rdd = sc.parallelize(list)
     
     rdd.foreach(println)
     
     rdd.distinct().foreach { println }
      sc.stop()
  }
}