package com.bjsxt.scala.spark.topn
import org.apache.spark.{SparkContext, SparkConf}





object GroupTopN {
   def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GroupTopN").setMaster("local")
    val sc = new SparkContext(conf)
    
    val lines=sc.textFile("scores.txt")
    
    val lineList=lines.map(x=>(x.split("\t")(0),x.split("\t")(1))).groupByKey()
    
    val topList=lineList.map(x=>{
      var t = List[Int]()
      for(a<-x._2){
        t = t.::(a.toInt)
      }
      t.sortBy { x => -x }.take(3)
    })
    topList.foreach { println }
   }
}