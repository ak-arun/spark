package com.ak

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

object Main {
  def main(args: Array[String]): Unit = {
    val hiveContext = new HiveContext(new SparkContext())
    val names = hiveContext.sql("show tables").map { x => x.getString(0) }.collect()
    for(name <- names){
      println(name)
    }
    hiveContext.sql("create temporary function rev as 'com.ak.hive.udf.StringRevUDF'");
    val select = hiveContext.sql("select rev(album),artist from sm");
    val collect = select.map { x => (x.getString(0),x.getString(1)) }.collect();
   
   for(collected <- collect){
     println(collected._1+" - "+collected._2)
   }
    // specify jar using --jars
   // if on local mode, the jar shud be in local
   // if in cluster mode, the jar should be in hdfs 
    
  }
}