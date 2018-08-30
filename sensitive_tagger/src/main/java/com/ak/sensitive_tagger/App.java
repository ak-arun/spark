package com.ak.sensitive_tagger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import scala.collection.Iterator;
import scala.collection.JavaConverters;

import com.ak.sensitive_tagger.entity.Rule;
import com.ak.sensitive_tagger.utils.RuleParser;

public class App {
	public static void main(String[] args) throws IOException {
		String ruleFileName = "rules.json";
		String csvFileName = "ak.csv";

		Map<String, List<String>> ruleMap = getRules(ruleFileName);
		
		SparkConf conf = new SparkConf();
		conf.setAppName("App1");
		// JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaSparkContext jsc = new JavaSparkContext("local[*]", "WC");
		SQLContext sqlContext = new SQLContext(jsc);

		DataFrame userDataFrame = sqlContext.read()
				.format("com.databricks.spark.csv")
				.option("inferSchema", "true").option("header", "false")
				.option("delimiter", ",").load(csvFileName);

		String query = "";

		List<String> columnIdentifier = new ArrayList<String>();
		
		for (StructField f : userDataFrame.schema().fields()) {
			for (String s : ruleMap.keySet()) {
				columnIdentifier.add(s + "_COL_"+ f.name());
				query = query + "sum(" + s + "(" + f.name() + "))/count("
						+ f.name() + ") as " + s +"_COL_"+ f.name() + ",";
			}
		}

		query = query.substring(0, query.lastIndexOf(","));
		query = ("select " + query + " from datasetTable");
		
		for (String ruleKey : ruleMap.keySet()) {
			sqlContext.udf().register(ruleKey, getUdf(ruleMap.get(ruleKey)),
					DataTypes.IntegerType);
		}

		userDataFrame.registerTempTable("datasetTable");
		
		Row[] rows = sqlContext.sql(query).collect();
		Row r = rows[0];
		scala.collection.immutable.Map<String, Object> returnMap = r
				.getValuesMap(JavaConverters
						.asScalaIteratorConverter(columnIdentifier.iterator()).asScala()
						.toSeq());
		Iterator<String> it = returnMap.keySet().iterator();
		while (it.hasNext()) {
			String key = it.next();
			if (Double.valueOf("" + returnMap.get(key).get()) >= 0.5) {
				System.out.println("TAG : " + key.split("_COL_")[0]
						+ " : column name/position : " + key.split("_COL_")[1]
						+ " : SCORE : "
						+ Double.valueOf("" + returnMap.get(key).get()) * 100);
			}
		}
	
		

	}

	private static UDF1<Object, Integer> getUdf(final List<String> patterns) {
		return new UDF1<Object, Integer>() {
			private static final long serialVersionUID = 1L;

			public Integer call(Object inStringObj) throws Exception {
				for(String pattern : patterns){
					if(inStringObj.toString().replaceAll(pattern, "").equalsIgnoreCase("")){
						return 1;
					}
				}
				return 0;
			}
		};
	}

	private static Map<String, List<String>> getRules(String ruleFileName)
			throws IOException {
		Map<String, List<String>> ruleMap = new HashMap<String, List<String>>();
		BufferedReader br = new BufferedReader(new FileReader(new File(
				ruleFileName)));
		String ruleLine = null;
		while ((ruleLine = br.readLine()) != null) {
			Rule rule = RuleParser.getRuleFromJson(ruleLine);
			ruleMap.put(rule.getRuleName(), rule.getPatterns());
		}
		br.close();
		return ruleMap;
	}
}
