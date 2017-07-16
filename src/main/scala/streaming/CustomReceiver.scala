package streaming

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._
import scala.util.Success
import scala.language.postfixOps

//
// This is almost the simplest custom receiver possible. It emits the same
// string every 100ms or so. Almost all the code is involved with making
// it a 'good citizen' in terms of stopping in an orderly fashion when asked
// to do so by the streaming context.
//
class CustomReceiver
  extends Receiver[String](StorageLevel.MEMORY_ONLY)
  with Serializable
{

  //
  // Two way communication with the receiver thread: one promise/future
  // pair to ask it to stop and another to find out when it has stopped.
  // This is initialized in onStart() to work around the fact that a
  // Receiver must be serializable -- see
  // https://issues.apache.org/jira/browse/SPARK-1785
  //
  private class Synchronization {
    // this is completed to request that the loop stop
    val promiseToTerminate = Promise[Unit]()
    val futureTermination = promiseToTerminate.future

    // this is completed when the loop has stopped
    val promiseToStop = Promise[Unit]()
    val futureStopping = promiseToStop.future
  }

  private var sync: Synchronization = null

  // starting is easy: start a receiver thread as onStart() must not block
  def onStart(): Unit = {
    println("*** starting custom receiver")
    sync = new Synchronization
    new Thread("Custom Receiver") {
      override def run() {
        receive()
      }
    }.start()
  }

  // stopping is a little harder: once onStop() exits this receiver will be
  // assumed to have quiesced -- so you need to signal the thread, and then
  // wait for it to stop -- arguably a timeout is appropriate here
  def onStop(): Unit = {
    println("*** stopping custom receiver")
    sync.promiseToTerminate.complete(Success(()))
    Await.result(sync.futureStopping, 1 second)
    println("*** stopped custom receiver")
  }

  // This receive loop is bit tricky in terms of termination. Certainly it
  // should stop if 'isStopped' is true, as the streaming context thinks the
  // receiver has stopped. But that's not enough -- the loop also needs to
  // exit if the streaming context has asked it to by calling onStop() above.
  //
  private def receive(): Unit = {
    while(!isStopped && !sync.futureTermination.isCompleted) {
      try {
        // make it a bit slow to stop
        Thread.sleep(100)
        store("hello")
      } catch {
        case e: Exception => {
          println("*** exception caught in receiver calling store()")
          e.printStackTrace()
        }
      }
    }
    sync.promiseToStop.complete(Success(()))
    println("*** custom receiver loop exited")
  }

}


object CustomStreaming {
  def main (args: Array[String]) {
    val conf =
      new SparkConf().setAppName("CustomSeqStreaming").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(1))

    // create the stream
    val stream = ssc.receiverStream(new CustomReceiver)

    // register for data
    stream.foreachRDD(r => {
      println(s"Items: ${r.count()} Partitions: ${r.partitions.size}")
    })

    println("*** starting streaming")
    ssc.start()

    new Thread("Delayed Termination") {
      override def run() {
        Thread.sleep(15000)
        println("*** stopping streaming")
        ssc.stop()
      }
    }.start()

    try {
      ssc.awaitTermination()
      println("*** streaming terminated")
    } catch {
      case e: Exception => {
        println("*** streaming exception caught in monitor thread")
      }
    }

    // wait a bit longer for the call to awaitTermination() to return
    Thread.sleep(5000)

  }
}
