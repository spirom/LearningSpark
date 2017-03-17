name := "LearningSpark"

version := "1.0"

fork := true

scalaVersion := "2.11.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.1.0"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.1.0"

// needed to make the hiveql examples run at least on Linux
javaOptions in run += "-XX:MaxPermSize=128M"

// note: tested with -java-home pointing to a JDK 1.7