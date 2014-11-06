package streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

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
      println(acc.count)
      println(r.count())
    })
    ssc.start()
    qm.populateQueue()
    while (true) {
      Thread.sleep(100)
    }
  }
}
