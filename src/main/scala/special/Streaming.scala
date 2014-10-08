package special

import java.io.File
import org.apache.spark.streaming._

import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Random

class FileMaker {

  val root = new File("c:" + File.separator + "temp" + File.separator + "streamFiles")
  makeExist(root)
  val prep = new File(root.getAbsolutePath() + File.separator + "prep")
  makeExist(prep)
  val dest = new File(root.getAbsoluteFile() + File.separator + "dest")
  makeExist(dest)

  def writeOutput(f: File) : Unit = {
    for (i <- 1 to 100) {
      val p = new java.io.PrintWriter(f)
      try {
        p.println(Random.nextInt)
      } finally {
        p.close()
      }
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
    }
  }
}

object Streaming {



  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("Streaming").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))
    val fm = new FileMaker()
    val stream =  ssc.textFileStream(fm.dest.getAbsolutePath())
    stream.foreachRDD(r => println(r.count()))
    fm.makeFiles()
    Thread.sleep(10000)
  }
}
