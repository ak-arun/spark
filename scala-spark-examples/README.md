### Instructions:

Inspired by https://github.com/H4ml3t/spark-scala-maven-boilerplate-project

Integrating spark scala with maven

The project just reads an HDFS file and writes it back to the HDFS

Have tried to use java code as well. This is just to give a skeleton for one of the existing applications
at my end.

Modify the class "MainExample.scala" writing your Spark code, then compile the project with the command:

```mvn clean package```

Inside the ```/target``` folder you will find the result fat jar called ```spark-scala-example-0.0.1-SNAPSHOT-jar-with-dependencies.jar```. In order to launch the Spark job use this command in a shell with a configured Spark environment:

    spark-submit --class com.ak.spark.scala.examples.driver.MainExample spark-scala-example-0.0.1-SNAPSHOT-jar-with-dependencies.jar

Currently the paths are hard coded. 
