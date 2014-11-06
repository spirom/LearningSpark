package streaming


import java.io.File

import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

//
// A utility for creating a sequence of files of integers in the file system
// so that Spark can treat them like a stream.
//
class FileMaker {

  private val root =
    new File("c:" + File.separator + "temp" + File.separator + "streamFiles")
  makeExist(root)

  private val prep =
    new File(root.getAbsolutePath() + File.separator + "prep")
  makeExist(prep)

  val dest =
    new File(root.getAbsoluteFile() + File.separator + "dest")
  makeExist(dest)

  // fill a file with integers
  private def writeOutput(f: File) : Unit = {
    val p = new java.io.PrintWriter(f)
    try {
      for (i <- 1 to 100) {
        p.println(Random.nextInt)
      }
    } finally {
      p.close()
    }
  }

  private def makeExist(dir: File) : Unit = {
    dir.mkdir()
  }

  // make the sequence of files by creating them in one place and renaming
  // them into the directory where Spark is looking for them
  // (file-based streaming requires "atomic" creation of the files)
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

object FileBasedStreaming {
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("FileBasedStreaming").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(1))
    val fm = new FileMaker()

    // create the stream
    val stream = ssc.textFileStream(fm.dest.getAbsolutePath())

    // register for data
    stream.foreachRDD(r => {
      println(r.count())
    })

    // start streaming
    ssc.start()

    // start producing files
    fm.makeFiles()

    while (true) {
      Thread.sleep(100)
    }
  }
}
