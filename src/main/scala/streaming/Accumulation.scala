package streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

//
// This shows a rather simple minded approach to accumulating the data
// coming from a stream each time a batch is processed. Instead of using a Spark
// Accumulator it uses a global variable. This can be useful technique in
// special situations but it's usually not a good idea and rarely the most
// efficient approach.
//
object Accumulation {
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("Accumulation").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    var acc = sc.parallelize(Seq(0), 4)

    val qm = new QueueMaker(sc, ssc)
    val stream = qm.inputStream
    stream.foreachRDD(r => {
      acc = acc ++ r
      println("Count in accumulator: " + acc.count)
      println("Batch size: " + r.count())
    })
    ssc.start()

    new Thread("Delayed Termination") {
      override def run() {
        qm.populateQueue()
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

    println("*** done")
  }
}
