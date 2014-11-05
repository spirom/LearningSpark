package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.{SparkContext, SparkConf}

//
// While there's an easy way to read JSON there isn't an easy way to
// write it out formatted.
//
// One approach is to translate a SchemaRDD to an RDD[String] by applying a
// formatting function to each Row of the SchemaRDD and then writing out the
// resulting StringRDD as text. The tricky part is
//
object OutputJSON {

  def formatItem(p:Pair[StructField, Any]) : String = {
    p match {
      case (sf, a) =>
        sf.dataType match {
          // leaving out some of the atomic types
          case StringType => "\"" + sf.name + "\":\"" + a + "\""
          case IntegerType => "\"" + sf.name + "\":" + a
          // This next line deals with nested JSON structures (not needed if flat)
          case StructType(s) => "\"" + sf.name + "\":" + formatStruct(s, a.asInstanceOf[Row])
        }
    }
  }

  // Format a single struct by iterating through the schema and the Row
  def formatStruct(schema: Seq[StructField], r: Row) : String = {
    val paired = schema.zip(r)
    "{" + paired.foldLeft("")((s, p) => (if (s == "") "" else (s + ", ")) + formatItem(p)) + "}"

  }

  // Simultaneously iterate through the schema and Row each time --
  // the top level of a Row is always a struct.
  def formatSchemaRDD(st: StructType, srdd: SchemaRDD): RDD[String] = {
    srdd.map(r => formatStruct(st.fields, r))
  }

  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("JSON").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // easy enough to query flat JSON
    val people = sqlContext.jsonFile("src/main/resources/data/flat.json")
    val strings =  formatSchemaRDD(people.schema, people)
    strings.foreach(println)

    val peopleAddr = sqlContext.jsonFile("src/main/resources/data/notFlat.json")
    val nestedStrings = formatSchemaRDD(peopleAddr.schema, peopleAddr)
    nestedStrings.foreach(println)
  }
}
