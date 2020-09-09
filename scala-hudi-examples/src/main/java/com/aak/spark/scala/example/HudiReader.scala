package com.aak.spark.scala.example

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.hudi.DataSourceReadOptions;

object HudiReader {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf();
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    val spark = SparkSession.builder().appName("Hudi Reader").master("local[*]").config(conf).getOrCreate();
    spark.read.format("org.apache.hudi").option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL).load("op_hudi_glue2" + "/*").show();
  }
}