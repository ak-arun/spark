package com.ak.df.example.usage

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.SparkConf

object Main {
def main(args: Array[String]): Unit = {
//    
//    val sc = new SparkContext("local[1]","Sample App")
//    val stringRdd = sc.textFile("dfLoad.txt")
//    val tupleRdd = stringRdd.map { x =>toTuple(x) }.map { x =>x.productIterator.mkString("\t") }.saveAsTextFile("filename.tsv")
////    val delimitedRdd = stringRdd.map { x => x.split("~") }.map { x => toTabDelimitedString(x) }
////    delimitedRdd.saveAsTextFile("rddStore.txt")
//    val sqlContext = new SQLContext(sc)
//val df = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "false").option("delimiter", "~").load("dfLoad.txt")
//    df.write.format("com.databricks.spark.csv").option("delimiter", "\t").save("dfStore.txt")
//    
//  
  
//  val sc = new SparkContext("local[1]","Sample App")
//  val v1: Vector = Vectors.dense(2.0,3.0,4.0)
//  val v2: Vector = Vectors.dense(5.0, 6.0, 7.0)
//  println(v1)
//  println(v2)
//  val list = new ListBuffer[Vector]()
//  list += v1
//  list += v2
//  
// val listRdd =  sc.parallelize(list)
// 
// 
// listRdd.saveAsObjectFile("vectorObjects")
// 
// val fileRdd = sc.objectFile[Vector]("vectorObjects")
// 
// for(v <- fileRdd.collect()){
//   println(v)
// }
  
 val config = new SparkConf()
 config.setMaster("local").
 set("spark.serializer","org.apache.spark.serializer.KryoSerializer").
 set("spark.kryo.classesToRegister", "org.apache.hadoop.io.LongWritable,org.apache.hadoop.io.Text").
 setAppName("anApp")
 
  val sc = new SparkContext(config)
  

  val sqlContext = new SQLContext(sc)
  val conf = new Configuration();
	conf.set("mapreduce.input.fileinputformat.inputdir", "files")
  val files = sc.newAPIHadoopRDD(conf, classOf[TextInputFormat], classOf[LongWritable], classOf[Text]);
  val headerLessRDD = files.filter(f => f._1.get!=0).values.map { x => Row.fromSeq(x.toString().split(",")) }
  val header = files.filter(f => f._1.get==0).first()._2.toString()
  val schema = StructType(header.split(",").map(fieldName => StructField(fieldName, StringType, true)))
  val dataFrame =sqlContext.createDataFrame(headerLessRDD, schema)
  dataFrame.write.format("com.databricks.spark.csv").option("delimiter", "~").save("parsed")
  
  
  //headerLess.saveAsTextFile("headerLess")
  
  
//  val sqlContext = new SQLContext(sc)
// val frame = sqlContext.read.format("com.databricks.spark.csv")
//       .option("header", "true")
//       .load("files")
//   frame.write.format("com.databricks.spark.csv").option("delimiter", "~").save("parsed")


}
  
//  def toTabDelimitedString(x : Array[String]) : String = {
//    x.mkString("\t")
//  }
//  
//  def toTuple(x:String) = {
//   val array = x.split("~")
//    (array(0),array(1),array(2),array(3))
//  }



  
 
}