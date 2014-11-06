package streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}


object Windowing {
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("Windowing").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(1))
    val qm = new QueueMaker(sc, ssc)

    // create the stream
    val stream = qm.inputStream

    // register for data -- a five second sliding window every two seconds
    stream.window(Seconds(5), Seconds(2)).foreachRDD(r => {
      if (r.count() == 0)
        println("Empty")
      else
        println("Count = " + r.count() + " min = " + r.min()+ " max = " + r.max())
    })

    // start streaming
    ssc.start()

    // start producing data
    qm.populateQueue()

    while (true) {
      Thread.sleep(100)
    }
  }
}