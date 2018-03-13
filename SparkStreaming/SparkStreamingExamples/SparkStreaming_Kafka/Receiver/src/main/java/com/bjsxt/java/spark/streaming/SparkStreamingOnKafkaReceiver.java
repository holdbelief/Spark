package com.bjsxt.java.spark.streaming;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class SparkStreamingOnKafkaReceiver {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SparkStreamingOnKafkaReceiver")
				/*
				 * 当在Eclipse中运行的时候，去掉setMaster("local[1]")的注释，在yarn或者Standalone中运行的时候，注释掉
				 */
//				.setMaster("local[2]")
				.set("spark.streaming.receiver.writeAheadLog.enable", "true");

		JavaStreamingContext jsc = null;

		try {
			jsc = new JavaStreamingContext(conf, Durations.seconds(5));
			/*
			 * WAL路径设置
			 */
			jsc.checkpoint("hdfs://mycluster/kafka_spark/receiverdata");

			Map<String, Integer> topicConsumerConcurrency = new HashMap<String, Integer>();
			/*
			 * Map的Key：消费的Topic的名字，本例中Topic1，代表消费Topic1这个Topic Map的Value：启动几个线程去执行Receiver
			 * Task，本例中1，代表使用1个线程执行Receiver Task
			 * 如果Value设置为1，那么setMaster("local[2]")，就至少要设置为2个线程，一个线程用于执行Receiver
			 * Task，另一个线程用于执行业务
			 * 如果Value设置为2，那么setMaster("local[3]")，就至少要设置为3个线程，两个线程用于执行Receiver
			 * Task，剩下一个线程用于执行业务
			 */
			topicConsumerConcurrency.put("Topic1", 1);
			JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(jsc,
					"faith-openSUSE:2181,faith-Kylin:2181,faith-Mint:2181", "MyFirstConsumerGroup",
					topicConsumerConcurrency);

			/*
			 * lines是一个kv格式的 k:offset v:values
			 */
			JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {

				private static final long serialVersionUID = -1955739045858623388L;

				@Override
				public Iterable<String> call(Tuple2<String, String> tuple) throws Exception {
					return Arrays.asList(tuple._2.split("\t"));
				}
			});

			JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

				private static final long serialVersionUID = -4180968474440524871L;

				@Override
				public Tuple2<String, Integer> call(String word) throws Exception {
					return new Tuple2<String, Integer>(word, 1);
				}
			});

			JavaPairDStream<String, Integer> wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

				private static final long serialVersionUID = 5167887209365658964L;

				@Override
				public Integer call(Integer v1, Integer v2) throws Exception {
					return v1 + v2;
				}
			});

			wordsCount.print();

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
		} finally {
			if (jsc != null) {
				jsc.awaitTermination();
			}
		}
	}
}
