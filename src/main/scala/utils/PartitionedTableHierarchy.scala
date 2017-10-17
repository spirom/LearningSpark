package utils

import java.io.File

/**
  * Some utilities for use my multiple examples that deal with directory
  * hierarchy, especially to illustrate partitioned tables stored in HDFS.
  */
object PartitionedTableHierarchy {

  /**
    * Clean up the output from the last run
    */
  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }

  /**
    * Count the number of generated files with the given suffix
    */
  def countRecursively(file: File, suffix:String): Int = {
    if (file.isDirectory) {
      val counts = file.listFiles.map(f => countRecursively(f, suffix))
      counts.toList.sum
    } else {
      if (file.getName.endsWith(suffix)) 1 else 0
    }
  }

  /**
    * Print the directory hierarchy
    */
  def printRecursively(file: File, indent: Int = 0) : Unit = {
    0.to(indent).foreach(i => print("  "))
    if (file.isDirectory) {
      println("Directory: " + file.getName)
      file.listFiles.foreach(f => printRecursively(f, indent + 1))
    } else {
      println("File: " + file.getName)
    }
  }

}
