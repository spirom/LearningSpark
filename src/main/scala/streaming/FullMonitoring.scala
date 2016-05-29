package streaming

import org.apache.spark.streaming.scheduler._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.language.postfixOps

// This very more complex listener monitors the full range of receiver behaviors.
// For a simple example, see Monitoring.scala.
private class FullListener
  extends StreamingListener
{

  private def showBatchInfo(action: String, info: BatchInfo) : Unit = {
    println("=== " + action + " batch with " + info.numRecords + " records")
  }

  override def onBatchCompleted
  (batchCompleted: StreamingListenerBatchCompleted) = synchronized {

    showBatchInfo("completed", batchCompleted.batchInfo)

  }

  override def onBatchStarted
  (batchCompleted: StreamingListenerBatchStarted) = synchronized {

    showBatchInfo("started", batchCompleted.batchInfo)

  }

  override def onBatchSubmitted
  (batchCompleted: StreamingListenerBatchSubmitted) = synchronized {

    showBatchInfo("submitted", batchCompleted.batchInfo)

  }

  override def onReceiverStarted
  (receiverStarted: StreamingListenerReceiverStarted) = synchronized {
    println("=== LISTENER: Stopped receiver " +
      receiverStarted.receiverInfo.name+ "' on stream '" +
      receiverStarted.receiverInfo.streamId + "'")
  }

  override def onReceiverStopped
  (receiverStopped: StreamingListenerReceiverStopped) = synchronized {
    println("=== LISTENER: Stopped receiver '" +
      receiverStopped.receiverInfo.name + "' on stream '" +
      receiverStopped.receiverInfo.streamId + "'"
    )
  }

}

object FullMonitoring {
  def main (args: Array[String]) {
    val conf =
      new SparkConf().setAppName("MonitoringStreaming").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(1))

    // create the stream
    val stream = ssc.receiverStream(new CustomReceiver)
    val stream2 = ssc.receiverStream(new CustomReceiver)


    // register a listener to monitor all the receivers
    val listener = new FullListener
    ssc.addStreamingListener(listener)

    // register for data
    stream.foreachRDD(r => {
      println(r.count())
    })

    stream2.foreachRDD(r => {
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



    // wait a bit longer for the call to awaitTermination() to return
    Thread.sleep(5000)

  }
}
