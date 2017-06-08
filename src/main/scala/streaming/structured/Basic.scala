package streaming.structured

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import streaming.util.CSVFileStreamGenerator

/**
  * A very basic example of structured streaming as introduced in Spark 2.0.
  *
  * A sequence of CSV files is treated as a stream and subscribed to,
  * producing a streaming DataFrame.
  *
  * In this example, every time a batch of data is delivered the new records are
  * dumped to the console. Keep in mind that some batches will deliver only
  * one file, while others will deliver several files.
  */

object Basic {

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

    // every time a batch of records is received, dump the new records
    // to the console -- often this will just eb the contents of a single file,
    // but sometimes it will contain mote than one file
    val query = csvDF.writeStream
      .outputMode("append")
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
