package sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object JSON {
  def main (args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("SQL-JSON")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    // easy enough to query flat JSON
    val people = spark.read.json("src/main/resources/data/flat.json")
    people.printSchema()
    people.createOrReplaceTempView("people")
    val young = spark.sql("SELECT firstName, lastName FROM people WHERE age < 30")
    young.foreach(r => println(r))

    // nested JSON results in fields that have compound names, like address.state
    val peopleAddr = spark.read.json("src/main/resources/data/notFlat.json")
    peopleAddr.printSchema()
    peopleAddr.foreach(r => println(r))
    peopleAddr.createOrReplaceTempView("peopleAddr")
    val inPA = spark.sql("SELECT firstName, lastName FROM peopleAddr WHERE address.state = 'PA'")
    inPA.foreach(r => println(r))

    // interesting characters in field names lead to problems with querying, as Spark SQL
    // has no quoting mechanism for identifiers
    val peopleAddrBad = spark.read.json("src/main/resources/data/notFlatBadFieldName.json")
    peopleAddrBad.printSchema()

    // instead read the JSON in as an RDD[String], do necessary string
    // manipulations (example below is simplistic) and then turn it into a Schema RDD
    val lines = spark.read.textFile("src/main/resources/data/notFlatBadFieldName.json")
    val linesFixed = lines.map(s => s.replaceAllLiterally("$", ""))
    val peopleAddrFixed = spark.read.json(linesFixed)
    peopleAddrFixed.printSchema()
    peopleAddrFixed.createOrReplaceTempView("peopleAddrFixed")
    val inPAFixed = spark.sql("SELECT firstName, lastName FROM peopleAddrFixed WHERE address.state = 'PA'")
    inPAFixed.foreach(r => println(r))
  }
}
