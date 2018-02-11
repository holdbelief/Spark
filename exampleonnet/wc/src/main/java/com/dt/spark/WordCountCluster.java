package com.dt.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class WordCountCluster {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("Wow, My First Spark App!");
		conf.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> rdd_lines = sc.textFile("hdfs://mycluster/user/faith/tmp/readme.txt");
		
		JavaRDD<String> rdd_word = rdd_lines.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" "));
			}
		});
		
		JavaPairRDD<String, Integer> rdd_word_1 = rdd_word.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
		JavaPairRDD<String, Integer> rdd_result = rdd_word_1.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		JavaPairRDD<Integer, String> rdd_revertKV = rdd_result.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
				return new Tuple2<Integer, String>(t._2, t._1);
			}
		});
		
		JavaPairRDD<Integer, String> sortByKey = rdd_revertKV.sortByKey();
		
		JavaRDD<Tuple2<String, Integer>> revertKVAgain = sortByKey.map(new Function<Tuple2<Integer,String>, Tuple2<String,Integer>>() {

			@Override
			public Tuple2<String,Integer> call(Tuple2<Integer, String> v1) throws Exception {
				return new Tuple2<String,Integer>(v1._2, v1._1);
			}
		});
		
		revertKVAgain.foreach(new VoidFunction<Tuple2<String,Integer>>() {

			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println("单词 \"" + t._1 + "\" 出现了" + t._2 + "次");
			}
		});
	}

}
