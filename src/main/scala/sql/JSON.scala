package sql

import org.apache.spark.{SparkContext, SparkConf}


object JSON {
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("JSON").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // easy enough to query flat JSON
    val people = sqlContext.jsonFile("src/main/resources/data/flat.json")
    people.printSchema()
    people.registerTempTable("people")
    val young = sqlContext.sql("SELECT firstName, lastName FROM people WHERE age < 30")
    young.foreach(println)

    // nested JSON results in fields that have compound names, like address.state
    val peopleAddr = sqlContext.jsonFile("src/main/resources/data/notFlat.json")
    peopleAddr.printSchema()
    peopleAddr.foreach(println)
    peopleAddr.registerTempTable("peopleAddr")
    val inPA = sqlContext.sql("SELECT firstName, lastName FROM peopleAddr WHERE address.state = 'PA'")
    inPA.foreach(println)

    // interesting characters in field names lead to problems with querying, as Spark SQL
    // has no quoting mechanism for identifiers
    val peopleAddrBad = sqlContext.jsonFile("src/main/resources/data/notFlatBadFieldName.json")
    peopleAddrBad.printSchema()

    // instead read the JSON in as an RDD[String], do necessary string
    // manipulations (example below is simplistic) and then turn it into a Schema RDD
    val lines = sc.textFile("src/main/resources/data/notFlatBadFieldName.json")
    val linesFixed = lines.map(s => s.replaceAllLiterally("$", ""))
    val peopleAddrFixed = sqlContext.jsonRDD(linesFixed)
    peopleAddrFixed.printSchema()
    peopleAddrFixed.registerTempTable("peopleAddrFixed")
    val inPAFixed = sqlContext.sql("SELECT firstName, lastName FROM peopleAddrFixed WHERE address.state = 'PA'")
    inPAFixed.foreach(println)
  }
}
