package sql

import java.sql.{Timestamp, Date}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, SparkConf}

object JSONTypes {
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("JSONTypes").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val schema = StructType(
      Seq(
        StructField("date",DateType, true),
        StructField("ts", TimestampType, true)
      )
    )
    val rows = sc.parallelize(Seq(Row(
      new Date(3601000),
      new Timestamp(3601000)
    )), 4)
    val tdf = sqlContext.createDataFrame(rows, schema)

    tdf.toJSON.foreach(println)


    val text = sc.parallelize(Seq(
    "{\"date\":\"1969-12-31\", \"ts\": \"1969-12-31 17:00:01.0\"}"
    ), 4)

    val json1 = sqlContext.read.json(text)

    json1.printSchema()

    // TODO: lost the ability to do this?
    /*
    val json2 = sqlContext.read.json(text, schema)

    json2.printSchema()
    json2.show()
    */

    val textConflict = sc.parallelize(Seq(
      "{\"key\":42}",
      "{\"key\":\"hello\"}",
      "{\"key\":false}"
    ), 4)

    val jsonConflict = sqlContext.read.json(textConflict)

    jsonConflict.printSchema()

    jsonConflict.registerTempTable("conflict")

    sqlContext.sql("SELECT * FROM conflict").show()


  }
}
