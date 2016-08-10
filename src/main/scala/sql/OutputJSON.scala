package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}


//
// NOTE: This is example is now, strictly speaking, out of date, as the
// ability to write JSON was added in Spark 1.2.0, but the example was written
// to answer the following question on StackOverflow in the days of Spark 1.1.0.
//
// http://stackoverflow.com/questions/26737251/pyspark-save-schemardd-as-json-file
//
// The techniques may still be of some interest.
//
// ======================================================================
//
// While there's an easy way to read JSON there isn't an easy way to
// write it out formatted.
//
// One approach is to translate a DataFrame to an RDD[String] by applying a
// formatting function to each Row of the DataFrame and then writing out the
// resulting StringRDD as text.
//
object OutputJSON {

  def formatItem(p:(StructField, Any)) : String = {
    p match {
      case (sf, a) =>
        sf.dataType match {
          // leaving out some of the atomic types
          case StringType => "\"" + sf.name + "\":\"" + a + "\""
          case IntegerType => "\"" + sf.name + "\":" + a
          case LongType => "\"" + sf.name + "\":" + a
          // This next line deals with nested JSON structures (not needed if flat)
          case StructType(s) => "\"" + sf.name + "\":" + formatStruct(s, a.asInstanceOf[Row])
        }
    }
  }

  // Format a single struct by iterating through the schema and the Row
  def formatStruct(schema: Seq[StructField], r: Row) : String = {
    val paired = schema.zip(r.toSeq)
    "{" + paired.foldLeft("")((s, p) => (if (s == "") "" else (s + ", ")) + formatItem(p)) + "}"

  }

  // Simultaneously iterate through the schema and Row each time --
  // the top level of a Row is always a struct.
  def formatDataFrame(st: StructType, srdd: DataFrame): RDD[String] = {
    srdd.rdd.map(formatStruct(st.fields, _))
  }

  def main (args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("SQL-OutputJSON")
        .master("local[4]")
        .getOrCreate()

    // easy enough to query flat JSON
    val people = spark.read.json("src/main/resources/data/flat.json")
    val strings =  formatDataFrame(people.schema, people)
    strings.foreach(println)

    val peopleAddr = spark.read.json("src/main/resources/data/notFlat.json")
    val nestedStrings = formatDataFrame(peopleAddr.schema, peopleAddr)
    nestedStrings.foreach(println)
  }
}
