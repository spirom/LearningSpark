name := "LearningSpark"

version := "1.0"

fork := true

scalaVersion := "2.11.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.0"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.6.0"

// needed to make the hiveql examples run at least on Linux
javaOptions in run += "-XX:MaxPermSize=128M"
