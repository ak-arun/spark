package com.ak.examples.sparkdynamodb

import org.apache.spark.sql.SparkSession
import com.audienceproject.spark.dynamodb.implicits.DynamoDBDataFrameReader

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
