package com.bjsxt.java.spark.core.bjsxttopn;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class GroupTopN {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf()
				.setMaster("local")
				.setAppName("GroupTopN");
		final JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> scoreRDD = sc.textFile("scores.txt");
		
		JavaPairRDD<String, Integer> clazz2ScoreRDD = scoreRDD.mapToPair(new PairFunction<String, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String scoreInfo) throws Exception {
				String[] splited = scoreInfo.split("\t");
				return new Tuple2<String, Integer>(splited[0],Integer.valueOf(splited[1]));
			}
		});
		JavaPairRDD<String, Iterable<Integer>> groupClazz2ScoreRDD = clazz2ScoreRDD.groupByKey();
		
		groupClazz2ScoreRDD.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {
				String clazz = tuple._1;
				Iterator<Integer> scoreIterator = tuple._2.iterator();
				
				Integer[] topn = new Integer[3];
				
				while(scoreIterator.hasNext()){
					Integer score = scoreIterator.next();
					
					for(int i = 0 ; i < topn.length ; i++){
						if(topn[i] == null){
							topn[i] = score;
							break;
						}else if(score > topn[i]){
							for(int j = topn.length-1 ; j > i ;j--){
								topn[j] = topn[j-1];
							}
							topn[i] = score;
							break;
						}
					}
				}
				
				System.out.println("Class Name:"+clazz);
				for(int i = 0 ; i < topn.length ; i++){
					System.out.println(topn[i]);
				}
			}
		});
		
		
		
		
		
		
		
		
	}
}
