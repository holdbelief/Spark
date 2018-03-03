package com.bjsxt.java.spark.streaming;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;

import com.google.common.base.Optional;

import scala.Tuple2;

public class TransformOperator_my {
	public static void main(String[] args) {
		final String checkpointDirectory = "hdfs://mycluster/user/faith/SparkStreaming/SparkStreamingOnHDFS";
		
		final SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TransformBlacklist");
		
		JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
			
			@Override
			public JavaStreamingContext create() {
				return createContext(checkpointDirectory, conf);
			}
		};
		
		final JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(checkpointDirectory, factory);
		

//		System.out.println("================== Creating new context ===================");
//		
//		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(15));
//		
//		// 先做一份模拟的黑名单RDD
//		List<Tuple2<String, Boolean>> blacklist = new ArrayList<Tuple2<String, Boolean>>();
//		blacklist.add(new Tuple2<String, Boolean>("tom", true));
//		JavaPairRDD<String, Boolean> blacklistRDD = jsc.sparkContext().parallelizePairs(blacklist);
//		
//		JavaReceiverInputDStream<String> adsClickLogDStream = jsc.socketTextStream("faith-Fedora", 9999);
//		
//		JavaPairDStream<String, String> userAdsClickLogDStream = adsClickLogDStream.mapToPair(
//				new PairFunction<String, String, String>() {
//
//					private static final long serialVersionUID = 5757108339684350174L;
//
//					@Override
//					public Tuple2<String, String> call(String adsClickLog) throws Exception {
//						return new Tuple2<String, String>(adsClickLog.split(" ")[1], adsClickLog);
//					}
//		});
//		
//		JavaDStream<String> validAdsClickLogDStream = userAdsClickLogDStream.transform(new Function<JavaPairRDD<String,String>, JavaRDD<String>>() {
//
//			private static final long serialVersionUID = -5663662449020562579L;
//
//			@Override
//			public JavaRDD<String> call(JavaPairRDD<String, String> userAdsClickLogRDD) throws Exception {
//				JavaPairRDD<String,Tuple2<String,Optional<Boolean>>> leftOuterJoin = userAdsClickLogRDD.leftOuterJoin(blacklistRDD);
//				JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filter = leftOuterJoin.filter(new Function<Tuple2<String,Tuple2<String,Optional<Boolean>>>, Boolean>() {
//					private static final long serialVersionUID = 3954946471247056575L;
//
//					@Override
//					public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
//						Optional<Boolean> option = tuple._2._2;
//						if ( option.isPresent() ) {
//							return !option.get();
//						}
//						return true;
//					}
//				});
//				
//				return filter.map(new Function<Tuple2<String, Tuple2<String,Optional<Boolean>>>, String>() {
//					private static final long serialVersionUID = -1318644877134058196L;
//
//					@Override
//					public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
//						return tuple._2._1;
//					}
//				});
//			}
//		});
//		
//		validAdsClickLogDStream.print();
		
		
	
		
		jsc.start();
		jsc.awaitTermination();
		jsc.close();
	}

	protected static JavaStreamingContext createContext(String checkpointDirectory, SparkConf conf) {
		System.out.println("================== Creating new context ===================");
		
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(15));
		jsc.checkpoint(checkpointDirectory);
		
		// 先做一份模拟的黑名单RDD
		List<Tuple2<String, Boolean>> blacklist = new ArrayList<Tuple2<String, Boolean>>();
		blacklist.add(new Tuple2<String, Boolean>("tom", true));
		JavaPairRDD<String, Boolean> blacklistRDD = jsc.sparkContext().parallelizePairs(blacklist);
		
		JavaReceiverInputDStream<String> adsClickLogDStream = jsc.socketTextStream("faith-Fedora", 9999);
		
		JavaPairDStream<String, String> userAdsClickLogDStream = adsClickLogDStream.mapToPair(
				new PairFunction<String, String, String>() {

					private static final long serialVersionUID = 5757108339684350174L;

					@Override
					public Tuple2<String, String> call(String adsClickLog) throws Exception {
						return new Tuple2<String, String>(adsClickLog.split(" ")[1], adsClickLog);
					}
		});
		
		JavaDStream<String> validAdsClickLogDStream = userAdsClickLogDStream.transform(new Function<JavaPairRDD<String,String>, JavaRDD<String>>() {

			private static final long serialVersionUID = -5663662449020562579L;

			@Override
			public JavaRDD<String> call(JavaPairRDD<String, String> userAdsClickLogRDD) throws Exception {
				JavaPairRDD<String,Tuple2<String,Optional<Boolean>>> leftOuterJoin = userAdsClickLogRDD.leftOuterJoin(blacklistRDD);
				JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filter = leftOuterJoin.filter(new Function<Tuple2<String,Tuple2<String,Optional<Boolean>>>, Boolean>() {
					private static final long serialVersionUID = 3954946471247056575L;

					@Override
					public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
						Optional<Boolean> option = tuple._2._2;
						if ( option.isPresent() ) {
							return !option.get();
						}
						return true;
					}
				});
				
				return filter.map(new Function<Tuple2<String, Tuple2<String,Optional<Boolean>>>, String>() {
					private static final long serialVersionUID = -1318644877134058196L;

					@Override
					public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
						return tuple._2._1;
					}
				});
			}
		});
		
		validAdsClickLogDStream.print();
		
		return jsc;
	}
}
