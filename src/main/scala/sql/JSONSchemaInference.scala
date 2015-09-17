package sql

import org.apache.spark.{SparkContext, SparkConf}

object JSONSchemaInference {
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("JSONSchemaInference").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // easy case -- one record
    val ex1 = sqlContext.read.json("src/main/resources/data/inference1.json")
    ex1.schema.printTreeString()
    ex1.registerTempTable("table1")
    println("simple query")
    sqlContext.sql("select b from table1").foreach(println)

    // two records, overlapping fields
    val ex2 = sqlContext.read.json("src/main/resources/data/inference2.json")
    ex2.schema.printTreeString()
    ex2.registerTempTable("table2")
    println("it's OK to reference a sometimes missing field")
    sqlContext.sql("select b from table2").foreach(println)
    println("it's OK to reach into a sometimes-missing record")
    sqlContext.sql("select g.h from table2").foreach(println)

    // two records, scalar and structural conflicts
    val ex3 = sqlContext.read.json("src/main/resources/data/inference3.json")
    ex3.schema.printTreeString()
    ex3.registerTempTable("table3")
    println("it's ok to query conflicting types but not reach inside them")
    // don't try to query g.h or g[1]
    sqlContext.sql("select g from table3").foreach(println)
  }
}
