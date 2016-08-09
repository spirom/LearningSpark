package streaming

import org.apache.spark.streaming.scheduler.{StreamingListenerBatchCompleted, StreamingListener}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.language.postfixOps

// This very simple listener gets called on every completed batch for
// streaming context on which it is registered. However, it sums the record
// count for only one stream ID, which is passed into the constructor.
// It can be called at any time to get the current record count.
private class SimpleListener(val streamId: Int)
  extends StreamingListener
{

  private var recordCounter: Long = 0

  def recordsProcessed = synchronized { recordCounter }

  override def onBatchCompleted
  (batchCompleted: StreamingListenerBatchCompleted) = synchronized {

    val optInfo = batchCompleted.batchInfo.streamIdToInputInfo.get(streamId)
    optInfo.foreach(info => recordCounter = recordCounter + info.numRecords)

  }

}

object Monitoring {
  def main (args: Array[String]) {
    val conf =
      new SparkConf().setAppName("MonitoringStreaming").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(1))

    // create the stream
    val stream = ssc.receiverStream(new CustomReceiver)

    // Register a listener to count the records passing through this
    // stream. Notice it's registered on the
    // streaming context, not the stream, so it monitors all the streams, and
    // hence all the receivers. But it only pays attention to the stream
    // we told it to pay attention to.
    val listener = new SimpleListener(stream.id)
    ssc.addStreamingListener(listener)

    // register for data
    stream.foreachRDD(r => {
      println(r.count())
    })

    println("*** starting streaming")
    ssc.start()

    println("*** starting termination monitor")

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

    Thread.sleep(10000)

    println("*** stopping streaming")
    ssc.stop()

    // get the record count from the listener
    println("*** records processed: " + listener.recordsProcessed)

    // wait a bit longer for the call to awaitTermination() to return
    Thread.sleep(5000)

    println("*** done")
  }
}
