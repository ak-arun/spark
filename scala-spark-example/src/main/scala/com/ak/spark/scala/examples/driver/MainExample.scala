package com.ak.spark.scala.examples.driver;

import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;

object MainExample {

  def main(arg: Array[String]) {

    /*var logger = Logger.getLogger(this.getClass())

    if (arg.length < 2) {
      logger.error("=> wrong parameters number")
      System.err.println("Usage: MainExample <path-to-files> <output-path>")
      System.exit(1)
    }

    val jobName = "MainExample"

    val conf = new SparkConf().setAppName(jobName)
    val sc = new SparkContext(conf)

    val pathToFiles = arg(0)
    val outputPath = arg(1)

    logger.info("=> jobName \"" + jobName + "\"")
    logger.info("=> pathToFiles \"" + pathToFiles + "\"")

    val files = sc.textFile(pathToFiles)

    // do your work here
    val rowsWithoutSpaces = files.map(_.replaceAll(" ", ","))

    // and save the result
    rowsWithoutSpaces.saveAsTextFile(outputPath)*/
    
    
    
    
    
    // ARUN
    
    var config = new SparkConf().setMaster("local[*]").setAppName("spark scala example 1");
    var sc = new SparkContext("local[*]","app1",config);
    var textFile = sc.textFile("hello.txt", 1);
    println(textFile.count());

  }
}
