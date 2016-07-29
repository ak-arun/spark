package com.ak.spark.examples;

import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.HashSet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

public class DataFramesAndJoins {

	
	public static void main(String[] args) throws FileNotFoundException {
		
		
		
		SparkConf conf = new SparkConf();
		conf.setAppName("Sample App - Uniq Location per item");
		JavaSparkContext jsc = new JavaSparkContext("local[*]","Sample App - Uniq Location per item");
		//JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(jsc);
		
		//id	email	language	location ----------- USER HEADERS
		DataFrame userDataFrame = sqlContext.read()
			    .format("com.databricks.spark.csv")
			    .option("inferSchema", "true")
			    .option("header", "true")
			    .option("delimiter", "\t")
			    .load("user");
		
		//txid	pid	uid	price	desc -------------------- TRANSACTION HEADERS
		DataFrame transactionDataFrame = sqlContext.read()
			    .format("com.databricks.spark.csv")
			    .option("inferSchema", "true")
			    .option("header", "true")
			    .option("delimiter", "\t")
			    .load("transactions");
		
		Column joinColumn = userDataFrame.col("id").equalTo(transactionDataFrame.col("uid"));
		
		DataFrame userTransactionFrame = userDataFrame.join(transactionDataFrame,joinColumn,"rightouter");
		
		DataFrame productIdLocationDataFrame = userTransactionFrame.select(userTransactionFrame.col("pid"),userTransactionFrame.col("location"));
		
	JavaRDD<Row> productIdLocationJavaRDD = productIdLocationDataFrame.toJavaRDD();
		
		JavaPairRDD<String, String> productIdLocationJavaPairRDD = productIdLocationJavaRDD.mapToPair(new PairFunction<Row, String, String>() {

			public Tuple2<String, String> call(Row inRow) throws Exception {
				return new Tuple2(inRow.get(0),inRow.get(1));
			}
		});
		
		
		JavaPairRDD<String, Iterable<String>> productLocationList = productIdLocationJavaPairRDD.groupByKey();
		
		JavaPairRDD<String, Iterable<String>> productUniqLocations = productLocationList.mapValues(new Function<Iterable<String>, Iterable<String>>() {

			public Iterable<String> call(Iterable<String> inputValues) throws Exception {
				return new HashSet<String>((Collection<? extends String>) inputValues);
			}
		});
		
		productUniqLocations.saveAsTextFile("productUniqLocations");
	}
	
}
