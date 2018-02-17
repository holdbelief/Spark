package com.bjsxt.java.spark.core.operator.test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

// 理解里面final使用的原因！

public class MapOperator {

	public static void main(String[] args) {
		/**
		 * SparkConf是设置spark运行时的环境变量，可以设置setMaster  setAppName，设置运行时所需要的资源情况
		 */
		SparkConf conf = new SparkConf().setAppName("JoinOperator")
				.setMaster("local");
		
		//SparkContext非常的重要，SparkContext是通往集群的唯一通道，在SparkContext初始化的时候会创建任务调度器
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.addFile("cs");
		
		String string = SparkFiles.get("cs");
		System.out.println(string);
		// 准备一下数据
		List<String> names = Arrays.asList("xurunyun","liangyongqi","wangfei");
		
		JavaRDD<String> nameRDD = sc.parallelize(names);
		
	 	Map<String, Integer> scoreMap = new HashMap<String, Integer>();
		scoreMap.put("xurunyun", 150);
		scoreMap.put("liangyongqi", 100);
		scoreMap.put("wangfei", 90);
		 
		
	/*	final Broadcast<Map<String, Integer>> mapBroadcast = sc.broadcast(scoreMap);
		
		final Accumulator<Integer> accumulator = sc.accumulator(0); */
		
		 JavaRDD<String> scoreRDD =nameRDD.map(new Function<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String v1) throws Exception {
				
					//数据库连接
//				Map<String, Integer> scoreMap = mapBroadcast.value();
//				Integer score = scoreMap.get(v1);
//				accumulator.add(score);
//				System.out.println(accumulator.value());
				return "hello "+v1;
			}
		});
		
		scoreRDD.foreach(new VoidFunction<String>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String score) throws Exception {
				System.out.println(score);
				
				
			}
		});
		
		while (true) {
			
		}
//		 System.out.println("accumulator:"+accumulator.value());
	}
}
