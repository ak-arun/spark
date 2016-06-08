package com.ak.spark.scala.examples.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;

public class Utils {

	public static Configuration getConfiguration(int recordLength, String filePath){
		Configuration conf = new Configuration();
		conf.set("fixedlengthinputformat.record.length",String.valueOf(recordLength));
		conf.set("mapreduce.input.fileinputformat.inputdir", filePath);
		return conf;
	}
	
	
	public static String performTransformation(BytesWritable b){
		return (new String(b.getBytes()));
		
	}
	
	
	/*    val m = values.map { x => 
    {
    val modified = ConfigUtil.toBeMoved(x); 
    modified; 
    }
}*/
}
