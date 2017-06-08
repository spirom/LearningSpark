package streaming.structured

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import streaming.util.CSVFileStreamGenerator

/**
  * A very basic example of structured streaming as introduced in Spark 2.0.
  *
  * A sequence of CSV files is treated as a stream and subscribed to,
  * producing a streaming DataFrame, and an aggregation is defined as
  * another streaming DataFrame.
  *
  * In this example, every time a batch of data is delivered the resulting
  * aggregation data is dumped to the console. Since the files all have
  * the same keys, each batch will have the same count for each key, but the
  * count increases by an unpredictable amount, since the number of files
  * delivered in each batch varies.
  */

object BasicAggregation {

  def main (args: Array[String]) {


   val fm = new CSVFileStreamGenerator(10, 5, 500)

    println("*** Starting to stream")

    val spark = SparkSession
      .builder
      .appName("StructuredStreaming_Basic")
      .config("spark.master", "local[4]")
      .getOrCreate()

    // schema for the streaming records
    val recordSchema = StructType(
      Seq(
        StructField("key", StringType),
        StructField("value", IntegerType)
      )
    )

    // a streaming DataFrame resulting from parsing the records of the CSV files
    val csvDF = spark
      .readStream
      .option("sep", ",")
      .schema(recordSchema)
      .format("csv")
      .load(fm.dest.getAbsolutePath)

    // it has the schema we specified
    csvDF.printSchema()

    // a streaming DataFrame that aggregates the original one, grouped by the key
    val countsDF = csvDF
      .groupBy("key")
      .count()

    // its schema is determined by the aggregation
    countsDF.printSchema()

    // every time a batch of records is received, dump the entire aggregation
    // result to the console
    val query = countsDF.writeStream
      .outputMode("complete") // only allowed when aggregating
      .format("console")
      .start()

    println("*** done setting up streaming")

    Thread.sleep(5000)

    println("*** now generating data")
    fm.makeFiles()

    Thread.sleep(5000)

    println("*** Stopping stream")
    query.stop()

    query.awaitTermination()
    println("*** Streaming terminated")

    println("*** done")
  }
}
