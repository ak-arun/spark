package com.ak.spark.java;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

public class RDDToDataFrame {

	public static void main(String[] args) {
		
		String logLine = "piweba4y.prodigy.com - - [01/Aug/1995:00:00:10 -0400] \"GET /images/launchmedium.gif HTTP/1.0\" 200 11853";
		
		List<String> logs = new ArrayList<String>();
		logs.add(logLine);
		logs.add(logLine);
		logs.add(logLine);
		logs.add(logLine);
		logs.add(logLine);
		
		
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("aName");
		conf.setAppName("Log Parser");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> logsRdd = jsc.parallelize(logs,1);
		
		
		
		JavaRDD<Row> rows = logsRdd.map(new Function<String, Row>() {
			public Row call(String log) throws Exception {
				String host = getAllMAtches(log, "^([^\\s]+\\s)").get(0);
				String time = getAllMAtches(log, "(\\d\\d/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2} -\\d{4})").get(0);
				String path = getAllMAtches(log, "^.*\"\\w+\\s+([^\\s]+)\\s+HTTP.*\"").get(0);
				String status = getAllMAtches(log, "\\s+(\\d{3})+\\s").get(0);
				String contentLength = getAllMAtches(log, "\\s+(\\d+$)").get(0);
				return RowFactory.create(host,time,path,status,contentLength);
			}
		});
		
		
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName : "host,time,path,status,contentLength".split(",")) {
		  StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
		  fields.add(field);
		}
		SQLContext sqlContext = new SQLContext(jsc);
		DataFrame dataFrame = sqlContext.createDataFrame(rows, DataTypes.createStructType(fields));
		dataFrame.write().format("com.databricks.spark.csv").option("delimiter", "~").save("apache-logs");
		

		
		
		
		
	}
	
	public static List<String> getAllMAtches(String string,String regex){
		// reusing an existing method. No need to return list for your case, just return first match
		 List<String> allMatches = new ArrayList<String>();
		 Matcher m = Pattern.compile(regex).matcher(string);
		 while (m.find()) {
		   allMatches.add(m.group());
		 }
		return allMatches;
	}

	
	
}
