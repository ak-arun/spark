package com.aak.spark.scala.example

import org.apache.spark.sql.SparkSession
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object HudiWriter {
  def main(args: Array[String]): Unit = {
    
    
   val conf = new  SparkConf();
   conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
   
    
    val spark = SparkSession.builder().appName("app 1").master("local[*]").config(conf).getOrCreate();
    
    spark.read.json("input.json")
      .write
      .format("org.apache.hudi")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "id")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "seq")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      .option(HoodieWriteConfig.TABLE_NAME, "op_hudi_glue2")
      .mode(SaveMode.Append)
      .save("op_hudi_glue2");

  }
}