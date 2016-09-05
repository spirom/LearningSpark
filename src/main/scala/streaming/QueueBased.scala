package streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

//
// A utility for streaming data through an in-memory queue
// See https://issues.apache.org/jira/browse/SPARK-17397
//
class QueueMaker(sc: SparkContext, ssc:StreamingContext) {

  private val rddQueue = new mutable.Queue[RDD[Int]]()

  val inputStream = ssc.queueStream(rddQueue)

  private var base = 1

  // each RDD has 100 different integers
  private def makeRDD() : RDD[Int] = {
    val rdd = sc.parallelize(base to base + 99 , 4)
    base = base + 100
    rdd
  }

  // put 10 RDDs in the queue
  def populateQueue() : Unit = {
    for (n <- 1 to 10) {
      rddQueue.enqueue(makeRDD())
    }
  }
}

// Queue based streaming emits the data of a single queue entry for each batch,
// even if many entries have been enqueued in advance. Notice in this example
// that the queue has been pre-populated with 10 entries, and they are emitted
// one per second.

object QueueBasedStreaming {
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("QueueBasedStreaming").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(1))
    val qm = new QueueMaker(sc, ssc)

    // create the stream
    val stream = qm.inputStream

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

    println("*** producing data")
    // start producing data
    qm.populateQueue()

    Thread.sleep(15000)

    println("*** stopping streaming")
    ssc.stop()

    // wait a bit longer for the call to awaitTermination() to return
    Thread.sleep(5000)

    println("*** done")
  }
}
