package com.ak.spark.examples;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {

	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf();
		conf.setAppName("First App - Word Count");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> fileRDD = jsc.textFile("hello.txt");
		JavaRDD<String> words = fileRDD.flatMap(new FlatMapFunction<String, String>() {

			public Iterable<String> call(String aLine) throws Exception {
				return Arrays.asList(aLine.split(" "));
			}
		});
		
		JavaPairRDD<String, Integer> wordOccurrence = words.mapToPair(new PairFunction<String, String, Integer>() {

			public Tuple2<String, Integer> call(String inWord) throws Exception {
				return new Tuple2<String, Integer>(inWord, 1);
			}
		});
		
		JavaPairRDD<String, Integer> summed = wordOccurrence.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			public Integer call(Integer a, Integer b) throws Exception {
				return a+b;
			}
		});
		
		jsc.stop();
		jsc.close();
		
	}
	
}
