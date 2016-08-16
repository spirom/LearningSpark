package dataframe

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

//
// Functions for querying against columns of DateType and TimestampType in
// a DataFrame.
//
object DateTime {
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("DataFrame-DateTime")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

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
          Date.valueOf("1999-01-11"),
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

    println("DataFrame with both DateType and TimestampType")
    tdf.show()

    println("Pull a DateType apart when querying")
    tdf.select($"dt", year($"dt"), quarter($"dt"), month($"dt"),
                weekofyear($"dt"), dayofyear($"dt"), dayofmonth($"dt")).show()

    println("Date arithmetic")
    tdf.select($"dt", datediff(current_date(), $"dt"),
                      date_sub($"dt", 20),
                      date_add($"dt", 10),
                      add_months($"dt", 6)).show()

    println("Date truncation")
    tdf.select($"dt", trunc($"dt", "YYYY"), trunc($"dt", "YY"),
                      trunc($"dt", "MM")).show()

    println("Date formatting")
    tdf.select($"dt", date_format($"dt", "MMM dd, YYYY")).show()

    println("Pull a Timestamp type apart when querying")
    tdf.select($"ts", year($"ts"), hour($"ts"), minute($"ts"), second($"ts")).show()
  }

}
