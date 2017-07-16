package sql

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

object JSONTypes {
  def main (args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("SQL-JSONTypes")
        .master("local[4]")
        .getOrCreate()

    val schema = StructType(
      Seq(
        StructField("date",DateType, true),
        StructField("ts", TimestampType, true)
      )
    )
    val rows = spark.sparkContext.parallelize(Seq(Row(
      new Date(3601000),
      new Timestamp(3601000)
    )), 4)
    val tdf = spark.createDataFrame(rows, schema)

    tdf.toJSON.foreach(r => println(r))


    val text = spark.sparkContext.parallelize(Seq(
    "{\"date\":\"1969-12-31\", \"ts\": \"1969-12-31 17:00:01.0\"}"
    ), 4)

    // needed for toDS() calls on RDDs below
    import spark.implicits._

    val json1 = spark.read.json(text.toDS())

    json1.printSchema()

    // TODO: lost the ability to do this?

    /*
    val json2 = spark.read.json(text, schema)

    json2.printSchema()
    json2.show()
    */

    val textConflict = spark.sparkContext.parallelize(Seq(
      "{\"key\":42}",
      "{\"key\":\"hello\"}",
      "{\"key\":false}"
    ), 4)

    val jsonConflict = spark.read.json(textConflict.toDS())

    jsonConflict.printSchema()

    jsonConflict.createOrReplaceTempView("conflict")

    spark.sql("SELECT * FROM conflict").show()


  }
}
