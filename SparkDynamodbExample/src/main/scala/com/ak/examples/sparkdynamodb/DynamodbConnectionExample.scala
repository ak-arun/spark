package com.ak.examples.sparkdynamodb

import org.apache.spark.SparkContext;
import org.apache.spark.sql.{ SaveMode, SparkSession };
import org.apache.hadoop.io.Text;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat
import org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io.LongWritable
import com.audienceproject.spark.dynamodb.implicits._
import org.apache.spark.sql.SparkSession

object DynamodbConnectionExample {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("DynamodbConnectionExample")
      .getOrCreate()
    val ak = "";
    val sk = "";
    System.setProperty("aws.accessKeyId", ak);
    System.setProperty("aws.secretAccessKey", sk);
    System.setProperty("aws.dynamodb.region", "us-east-2");
    System.setProperty("dynamodb.endpoint", "dynamodb.us-east-2.amazonaws.com");
    val dynamoDf = spark.read.dynamodb("aak");
    dynamoDf.printSchema();

  }
}
