name         := "DataFrame-Example"
version      := "1.0"
organization := "com.ak"
scalaVersion := "2.10.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"
scalaSource in Compile := baseDirectory.value / "src"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.4.0"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.6.1"


