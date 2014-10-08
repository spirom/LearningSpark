package special

import java.io.File
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable
import scala.util.Random

class FileMaker {

  val root = new File("c:" + File.separator + "temp" + File.separator + "streamFiles")
  makeExist(root)
  val prep = new File(root.getAbsolutePath() + File.separator + "prep")
  makeExist(prep)
  val dest = new File(root.getAbsoluteFile() + File.separator + "dest")
  makeExist(dest)

  def writeOutput(f: File) : Unit = {
    val p = new java.io.PrintWriter(f)
    try {
      for (i <- 1 to 100) {
        p.println(Random.nextInt)
      }
    } finally {
      p.close()
    }
  }

  def makeExist(dir: File) : Unit = {
    dir.mkdir()
  }

  def makeFiles() = {
    for (n <- 1 to 10) {
      val f = File.createTempFile("Spark_", ".txt", prep)
      writeOutput(f)
      val nf = new File(dest + File.separator + f.getName())
      f renameTo nf
      nf.deleteOnExit()
      Thread.sleep(500)
    }
  }
}

class QueueMaker(sc: SparkContext, ssc:StreamingContext) {

  val rddQueue = new mutable.SynchronizedQueue[RDD[Int]]()

  val inputStream = ssc.queueStream(rddQueue)

  def makeRDD() : RDD[Int] = {
    sc.parallelize(for (i <- 1 to 100) yield Random.nextInt, 4)
  }

  def populateQueue() : Unit = {
    for (n <- 1 to 10) {
      rddQueue.enqueue(makeRDD())
    }
  }
}

object Streaming {
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("Streaming").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))
    val fm = new FileMaker()
    val stream = ssc.textFileStream(fm.dest.getAbsolutePath())
    //val qm = new QueueMaker(sc, ssc)
    //val stream = qm.inputStream
    stream.foreachRDD(r => {
      val c = r.count()
      if (c == 0) {
        ssc.stop()
        return
      }
      println(r.count())
    })
    ssc.start()
    fm.makeFiles()
    //qm.populateQueue()
    while (true) {
      Thread.sleep(100)
    }
  }
}
