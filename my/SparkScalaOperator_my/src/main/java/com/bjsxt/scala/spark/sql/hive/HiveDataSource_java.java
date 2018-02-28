package com.bjsxt.scala.spark.sql.hive;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

public class HiveDataSource_java {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("HiveDataSource_java");
		JavaSparkContext sc = new JavaSparkContext(conf);
		HiveContext hiveContext = new HiveContext(sc);
		
		hiveContext.sql("show databases").show();
	    
	    /*
	     * cascade 关键字，如果没有这个关键字，当数据库里面有表的时候，会抛出异常
	     * InvalidOperationException(message:Database sparkonhive is not empty. One or more tables exist.)
	     */
	    hiveContext.sql("DROP DATABASE IF EXISTS SparkOnHive CASCADE"); 
	    hiveContext.sql("CREATE DATABASE SparkOnHive");
	    hiveContext.sql("use SparkOnHive");
	    hiveContext.sql("DROP TABLE IF EXISTS student_infos");
	    hiveContext.sql("CREATE TABLE IF NOT EXISTS student_infos (" +
	                        " id INT, " + 
	                        " name STRING," + 
	                        " age INT, " + 
	                        " address STRING)" +
	                        " ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
	                        
	    hiveContext.sql("LOAD DATA LOCAL INPATH '/home/faith/tmp/student_infos' INTO TABLE student_infos");
	    
	    hiveContext.sql("DROP TABLE IF EXISTS student_scores");
	    hiveContext.sql("CREATE TABLE IF NOT EXISTS student_scores (" + 
		                      " name STRING," +
	                        " score INT)" +
		                      " ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
		                      
		  hiveContext.sql("LOAD DATA "
	                  + "LOCAL INPATH '/home/faith/tmp/student_scores' "
	                  + "INTO TABLE student_scores");                    
		                      
		  DataFrame goodStudentsDF = hiveContext.sql("SELECT si.name, si.age, ss.score " + 
		                      " FROM student_infos si " + 
		                      " JOIN student_scores ss ON si.name = ss.name" + 
		                      " WHERE ss.score >= 80");
		                      
	    hiveContext.sql("DROP TABLE IF EXISTS good_student_infos");
	    
	    // 将goodStudentsDF里面的值写入到Hive表中，如果表不存在，会自动创建然后将数据插入到表中
	    goodStudentsDF.write().saveAsTable("good_student_infos");
	    
//	    while ( true ) {
//	    try {
//			Thread.sleep(1000);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	    }
	    
	    sc.stop();
	}
}
