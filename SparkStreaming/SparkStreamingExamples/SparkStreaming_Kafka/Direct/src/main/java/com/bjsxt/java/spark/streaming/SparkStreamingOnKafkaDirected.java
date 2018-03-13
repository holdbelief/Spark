package com.bjsxt.java.spark.streaming;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * 并行度问题：
 * 1、linesDStram里面封装到的是RDD， RDD里面有partition与这个topic的parititon数是一致的。
 * 2、从kafka中读来的数据封装一个DStram里面，可以对这个DStream重分区 reaprtitions(numpartition)
 * @author faith
 */
public class SparkStreamingOnKafkaDirected {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				/*
				 * Receiver模式下，需要使用一（数量可以设置，例如N个）个线程执行ReceiverTask，所以
				 * setMaster("local[2]")至少要是2个（N+1个）线程。
				 * 而Direct模式下，由于SparkStreaming直接从Kafka中取数据，没有ReceiverTask，所以最少可以设置1个线程
				 * 当在Eclipse中运行的时候，去掉setMaster("local[1]")的注释，在yarn或者Standalone中运行的时候，注释掉
				 */
//				.setMaster("local[1]")
				.setAppName("SparkStreamingOnKafkaDirected");

		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
		
		/*
		 * 第一个泛型是Kafka消息偏移量的数据类型
		 * 第二个泛型是Kafka消息体的数据类型
		 */
		Map<String, String> kafkaParameters = new HashMap<String, String>();
		
		/*
		 * metadata.broker.list，Kafka Broker的列表
		 */
		kafkaParameters.put("metadata.broker.list", "faith-openSUSE:9092,faith-Kylin:9092,faith-Mint:9092");
		
		HashSet<String> topics = new HashSet<String>();
		topics.add("Topic1");
		
		JavaPairInputDStream<String,String> lines = KafkaUtils.createDirectStream(jsc,
				String.class, // Kafka消息偏移量的数据类型
				String.class, // Kafka消息体的数据类型
				StringDecoder.class, // Kafka消息偏移量的反序列化处理类
				StringDecoder.class, // Kafka消息的反序列化处理类
				kafkaParameters,
				topics);
		
		JavaPairDStream<String,String> lines_repartition = lines.repartition(10);
		
		JavaDStream<String> words = lines_repartition.flatMap(new FlatMapFunction<Tuple2<String,String>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(Tuple2<String, String> tuple) throws Exception {
				return Arrays.asList(tuple._2.split(" "));
			}
		});
		
		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 7498759112557640377L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
		JavaPairDStream<String, Integer> wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = -6334959299984344333L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
			
		});
		
		/*
		 * 将word_count的内容写入Mysql
		 */
		wordsCount.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
			
			@Override
			public void call(JavaPairRDD<String, Integer> pairRdd) throws Exception {
				/*
				 * foreahPartition与foreach的不同是：
				 * foreach：每循环一次是一个数据
				 * foreachPartition：每循环一次是一个Partition，处理一个Partition里面的所有数据，
				 * 					注意VoidFunction的参数是Iterator，也就是每隔Partition里面数据的迭代
				 * foreachPartition常常用于向数据库例如数据，当使用foreach时候，每次循环是一个数据，那么每个数据
				 * 就要创建一个数据链连接，foreachPartition是每一个Partition创建一个数据库链接。
				 */
				pairRdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Integer>>>() {

					@Override
					public void call(Iterator<Tuple2<String, Integer>> vs) throws Exception {
						JDBCWrapper jdbcWrapper = JDBCWrapper.getJDBCInstance();
						List<Object[]> insertParams = new ArrayList<Object[]>();
						while ( vs.hasNext() ) {
							Tuple2<String, Integer> next = vs.next();
							insertParams.add(new Object[] {next._1, next._2});
						}
						
						if ( !insertParams.isEmpty() ) {
							System.out.println(insertParams);
							jdbcWrapper.doBatch("INSERT INTO wordcount VALUES(?, ?)", insertParams);
						}
					}
				});
			}
		});
		
		jsc.start();
		jsc.awaitTermination();
	}

}
