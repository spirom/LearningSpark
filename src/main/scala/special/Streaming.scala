package special

import java.io.File

import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Random

object Streaming {

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

  def makeExist(dir: String, empty: Boolean) : Unit = {
    // TODO: make sure it exists
    // TODO: clean if needed
  }

  def makeFiles() : Unit = {
    val root = "c:/temp/streamFiles"
    makeExist(root, false)
    val prep = root + "/prep"
    makeExist(prep, false)
    val dest = root + "/dest"
    makeExist(dest, true)
    for (n <- 1 to 10) {
      val localName = "f_" + n + ".txt"
      val f = new File(prep + "/" + localName)
      f renameTo new File(dest + "/" + localName)
    }
  }

  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("Streaming").setMaster("local[4]")
    val sc = new SparkContext(conf)
  }
}
