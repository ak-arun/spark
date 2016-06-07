package com.ak.spark.scala.examples.driver;

import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import com.ak.spark.scala.examples.util.Utils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.TextOutputFormat;

object MainExample {

  def main(arg: Array[String]) {
    var sc = new SparkContext("local[*]", "app1");
    val values = sc.newAPIHadoopRDD(Utils.getConfiguration(10, "hello.txt"), classOf[com.ak.spark.scala.examples.inputformat.FixedLengthInputFormat], classOf[LongWritable], classOf[BytesWritable]).values;
    val converted = values.map { x => (Utils.performTransformation(x), "") }
    converted.saveAsHadoopFile("opp", classOf[String], classOf[String], classOf[TextOutputFormat[String, String]]);
  }
}
