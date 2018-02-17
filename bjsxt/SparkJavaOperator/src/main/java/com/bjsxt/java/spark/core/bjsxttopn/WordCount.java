package com.bjsxt.java.spark.core.bjsxttopn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class WordCount {
	public static void main(String[] args) {
		//配置Spark 应用程序运行所需要的资源情况
		SparkConf conf = new SparkConf();
		conf.setAppName("WordCount");
		conf.setMaster("local");
		
		//SparkContext对象是通往集群的唯一通道
		JavaSparkContext context = new JavaSparkContext(conf);
		
		JavaRDD<String> rdd = context.textFile("hdfs://hadoop1:9000/user/bjsxt/agang/wordcount");
		
		JavaRDD<String> flatMapRDD = rdd.flatMap(new FlatMapFunction<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" "));
			}
		});
		
		JavaPairRDD<String, Integer> pairRDD = flatMapRDD.mapToPair(new PairFunction<String, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word,1);
			}
		});
		
		JavaPairRDD<String, Integer> resultRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				System.out.println("aaaaaaaaaaaaaa");
				return v1 + v2;
			}
		});
		
		resultRDD.foreach(new VoidFunction<Tuple2<String,Integer>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t);
			}
		});
		
		
		//关闭context  释放资源
		context.stop();
	}
}
