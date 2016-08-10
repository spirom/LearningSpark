package sql

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

//
// One way to create a DataFrame containing dates and timestamps, and
// query for ranges thereof.
//
object DateTime {
  def main (args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("SQL-DateTime")
        .master("local[4]")
        .getOrCreate()

    val schema = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("dt", DateType, true),
        StructField("ts", TimestampType, true)
      )
    )
    val rows = spark.sparkContext.parallelize(
      Seq(
        Row(
          1,
          Date.valueOf("2000-01-11"),
          Timestamp.valueOf("2011-10-02 09:48:05.123456")
        ),
        Row(
          1,
          Date.valueOf("2004-04-14"),
          Timestamp.valueOf("2011-10-02 12:30:00.123456")
        ),
        Row(
          1,
          Date.valueOf("2008-12-31"),
          Timestamp.valueOf("2011-10-02 15:00:00.123456")
        )
      ), 4)
    val tdf = spark.createDataFrame(rows, schema)

    tdf.printSchema()

    tdf.createOrReplaceTempView("dates_times")

    println("*** Here's the whole table")
    spark.sql("SELECT * FROM dates_times").show()

    println("*** Query for a date range")
    spark.sql(
      s"""
         |  SELECT * FROM dates_times
         |  WHERE dt > cast('2002-01-01' as date)
         |    AND dt < cast('2006-01-01' as date)
       """.stripMargin).show()

    println("*** Query to skip a timestamp range")
    spark.sql(
      s"""
         |  SELECT * FROM dates_times
         |  WHERE ts < cast('2011-10-02 12:00:00' as timestamp)
         |     OR ts > cast('2011-10-02 13:00:00' as timestamp)
       """.stripMargin).show()
  }

}
