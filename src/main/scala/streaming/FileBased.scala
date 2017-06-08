package streaming


import java.io.File

import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}
import streaming.util.CSVFileStreamGenerator

import scala.util.Random


//
// File based streaming requires files to be atomically created in
// the source directory -- in practice this entails creating them somewhere
// else and renaming them in place. Every batch emitted by the StreamingContext
// produces all the data from all files that have appeared since the last
// batch -- potentially many files are combined into a single RDD each time.
//

object FileBasedStreaming {
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("FileBasedStreaming").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(1))
    val fm = new CSVFileStreamGenerator(10, 100, 500)

    // create the stream
    val stream = ssc.textFileStream(fm.dest.getAbsolutePath)

    // register for data
    stream.foreachRDD(r => {
      println(r.count())
    })

    // start streaming
    ssc.start()

    new Thread("Streaming Termination Monitor") {
      override def run() {
        try {
          ssc.awaitTermination()
        } catch {
          case e: Exception => {
            println("*** streaming exception caught in monitor thread")
            e.printStackTrace()
          }
        }
        println("*** streaming terminated")
      }
    }.start()

    println("*** started termination monitor")

    // A curious fact about files based streaming is that any files written
    // before the first RDD is produced are ignored. So wait longer than
    // that before producing files.
    Thread.sleep(2000)
    println("*** producing data")
    // start producing files
    fm.makeFiles()

    Thread.sleep(10000)

    println("*** stopping streaming")
    ssc.stop()

    // wait a bit longer for the call to awaitTermination() to return
    Thread.sleep(5000)

    println("*** done")
  }
}
