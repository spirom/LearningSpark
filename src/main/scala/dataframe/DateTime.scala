package dataframe

import java.sql.{Timestamp, Date}

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._

//
// Interesting way to query against columns of DateType and TimestampType in
// a DataFrame.
//
object DateTime {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataFrame-DateTime").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val schema = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("dt", DateType, true),
        StructField("ts", TimestampType, true)
      )
    )
    val rows = sc.parallelize(
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
    val tdf = sqlContext.createDataFrame(rows, schema)

    println("DataFrame with both DateType and TimestampType")
    tdf.show()

    println("Pull a DateType apart when querying")
    tdf.select($"dt", year($"dt"), quarter($"dt"), month($"dt"),
                weekofyear($"dt"), dayofyear($"dt"), dayofmonth($"dt")).show()
  }

}
