package streaming.util

import java.io.File

import scala.util.Random

/**
  * A utility for creating a sequence of files of integers in the file system
  * so that Spark can treat them like a stream. This follows a standard pattern
  * to ensure correctness: each file is first created in another folder and then
  * atomically renamed into the destination folder so that the file's point of
  * creation is unambiguous, and is correctly recognized by the streaming
  * mechanism.
  *
  * Each generated file has the same number of key/value pairs, where the
  * keys have the same names from file to file, and the values are random
  * numbers, and thus vary from file to file.
  *
  * This class is used by several of the streaming examples.
  */
class CSVFileStreamGenerator(nFiles: Int, nRecords: Int, betweenFilesMsec: Int) {

  private val root =
    new File(File.separator + "tmp" + File.separator + "streamFiles")
  makeExist(root)

  private val prep =
    new File(root.getAbsolutePath() + File.separator + "prep")
  makeExist(prep)

  val dest =
    new File(root.getAbsoluteFile() + File.separator + "dest")
  makeExist(dest)

  // fill a file with integers
  private def writeOutput(f: File): Unit = {
    val p = new java.io.PrintWriter(f)
    try {
      for (i <- 1 to nRecords) {
        p.println(s"Key_$i,${Random.nextInt}")
      }
    } finally {
      p.close()
    }
  }

  private def makeExist(dir: File): Unit = {
    dir.mkdir()
  }

  // make the sequence of files by creating them in one place and renaming
  // them into the directory where Spark is looking for them
  // (file-based streaming requires "atomic" creation of the files)
  def makeFiles() = {
    for (n <- 1 to nFiles) {
      val f = File.createTempFile("Spark_", ".txt", prep)
      writeOutput(f)
      val nf = new File(dest + File.separator + f.getName)
      f renameTo nf
      nf.deleteOnExit()
      Thread.sleep(betweenFilesMsec)
    }
  }


}
