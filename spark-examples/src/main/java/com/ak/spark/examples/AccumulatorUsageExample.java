package com.ak.spark.examples;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class AccumulatorUsageExample {

	
	public static void main(String[] args) throws FileNotFoundException {
		
		SparkConf conf = new SparkConf();
		conf.setAppName("Third App - Word Count WITH BroadCast and Accumulator");
		//JavaSparkContext jsc = new JavaSparkContext("local[*]","A 1");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> fileRDD = jsc.textFile("hello.txt");
		JavaRDD<String> words = fileRDD.flatMap(new FlatMapFunction<String, String>() {

			public Iterable<String> call(String aLine) throws Exception {
				return Arrays.asList(aLine.split(" "));
			}
		});
		
		String[] stopWordArray = getStopWordArray();
		
		 final Accumulator<Integer> skipAccumulator = jsc.accumulator(0);
		 final Accumulator<Integer> unSkipAccumulator = jsc.accumulator(0);
		
		final Broadcast<String[]> stopWordBroadCast = jsc.broadcast(stopWordArray);
		
		
		JavaRDD<String> filteredWords = words.filter(new Function<String, Boolean>() {
			
			public Boolean call(String inString) throws Exception {
				boolean filterCondition = !Arrays.asList(stopWordBroadCast.getValue()).contains(inString);
				if(!filterCondition){
					System.out.println("Filtered a stop word ");
					skipAccumulator.add(1);
				}else{
					unSkipAccumulator.add(1);
				}
				return filterCondition;
				
			}
		});
		
		
		filteredWords.first(); // to trigger the action forcefully
		
		System.out.println("$$$$$$$$$$$$$$$Filtered Count "+skipAccumulator.value());
		System.out.println("$$$$$$$$$$$$$$$ UN Filtered Count "+unSkipAccumulator.value());
		
		JavaPairRDD<String, Integer> wordOccurrence = filteredWords.mapToPair(new PairFunction<String, String, Integer>() {

			public Tuple2<String, Integer> call(String inWord) throws Exception {
				return new Tuple2<String, Integer>(inWord, 1);
			}
		});
		
		JavaPairRDD<String, Integer> summed = wordOccurrence.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			public Integer call(Integer a, Integer b) throws Exception {
				return a+b;
			}
		});
		
		
		
		summed.saveAsTextFile("hello-out");
		jsc.stop();
		jsc.close();
		
	}
	
	static String[] getStopWordArray() throws FileNotFoundException {
	    Scanner stopWordScanner = new Scanner(new File("stop.txt"));
	    ArrayList<String> stopWordList = new ArrayList<String>();
	    while (stopWordScanner.hasNextLine()) {
	      stopWordList.add(stopWordScanner.nextLine());
	    }
	    return stopWordList.toArray(new String[0]);
	  }
	
}
