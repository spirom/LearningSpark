package streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * This example demonstrates that there doesn't seem to be a tidy way to
  * block the main thread until streaming has stopped if stream
  * processing is throwing exceptions.
  */

object ExceptionPropagation {
  case class SomeException(s: String)  extends Exception(s)

  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("ExceptionPropagation").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(1))
    val qm = new QueueMaker(sc, ssc)

    // create the stream
    val stream = qm.inputStream

    // register for data
    stream
      .map(x => { throw new SomeException("something"); x} )
      .foreachRDD(r => println("*** count = " + r.count()))

    // start streaming
    ssc.start()

    new Thread("Delayed Termination") {
      override def run() {
        Thread.sleep(30000)
        ssc.stop()
      }
    }.start()

    println("*** producing data")
    // start producing data
    qm.populateQueue()

    try {
      ssc.awaitTermination()
      println("*** streaming terminated")
    } catch {
      case e: Exception => {
        println("*** streaming exception caught in monitor thread")
      }
    }

    println("*** done")
  }
}
