name         := "spark-hive-example"
version      := "1.0"
organization := "com.ak"
scalaVersion := "2.10.0"
retrieveManaged := true
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % "provided"
scalaSource in Compile := baseDirectory.value / "src"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.1"
