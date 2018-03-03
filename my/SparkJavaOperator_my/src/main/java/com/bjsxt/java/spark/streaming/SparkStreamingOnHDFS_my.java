package com.bjsxt.java.spark.streaming;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;

import scala.Tuple2;

/**
*
*  Spark standalone or Mesos with cluster deploy mode only:
*  在提交application的时候  添加 --supervise 选项  如果Driver挂掉 会自动启动一个Driver
*  	SparkStreaming
* @author root
*
*/
public class SparkStreamingOnHDFS_my  implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -9131895644093923971L;

	public static void main(String[] args) {
		final SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("SparkStreamingOnHDFS_my");
		final String checkpointDirectory = "hdfs://mycluster/user/faith/SparkStreaming/SparkStreamingOnHDFS";
		
		JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
			@Override
			public JavaStreamingContext create() {
				return createContext(checkpointDirectory, conf);
			}
		};
		
		JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(checkpointDirectory, factory);

		jsc.start();
		jsc.awaitTermination();
	}
	
	private static JavaStreamingContext createContext(String checkpointDirectory, SparkConf conf) {
		System.out.println("Creating new context");
		SparkConf sparkConf = conf;
		
		JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(15));
		
		jsc.checkpoint(checkpointDirectory);
		
		JavaDStream<String> lines = jsc.textFileStream("hdfs://mycluster/hdfs/");
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = -8045207442298129092L;

			@Override
			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" "));
			}
		});
		
		JavaPairDStream<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {


			/**
			 * 
			 */
			private static final long serialVersionUID = 4046221734158013196L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
		JavaPairDStream<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = -9063259109173537669L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		counts.print();
		
		return jsc;
	}

}





