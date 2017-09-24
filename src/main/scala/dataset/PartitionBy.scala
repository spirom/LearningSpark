package dataset

import java.io.File

import java.io.File
import org.apache.spark.sql.SparkSession


object PartitionBy {

  case class Transaction(id: Long, year: Int, month: Int, day: Int,
                         quantity: Long, price: Double)

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }

  private def countRecursively(file: File, suffix:String): Int = {
    if (file.isDirectory) {
      val counts = file.listFiles.map(f => countRecursively(f, suffix))
      counts.toList.sum
    } else {
      if (file.getName().endsWith(suffix)) 1 else 0
    }
  }

  def main(args: Array[String]) {

    val exampleRoot = "/tmp/LearningSpark"

    val spark =
      SparkSession.builder()
        .appName("Dataset-PartitionBy")
        .master("local[4]")
        .config("spark.default.parallelism", 8)
        .getOrCreate()

    //spark.conf.set("spark.default.parallelism", 16)

    deleteRecursively(new File(exampleRoot))

    import spark.implicits._

    val transactions = Seq(
      Transaction(1001, 2016, 11, 5, 100, 42.99),
      Transaction(1002, 2016, 11, 15, 50, 75.95),
      Transaction(1003, 2016, 11, 15, 50, 19.95),
      Transaction(1004, 2016, 11, 15, 25, 42.99),
      Transaction(1005, 2016, 12, 11, 99, 11.00),
      Transaction(1006, 2016, 12, 22, 10, 10.99),
      Transaction(1007, 2016, 12, 22, 10, 75.95),
      Transaction(1008, 2017, 1, 2, 1020, 9.99),
      Transaction(1009, 2017, 1, 2, 100, 19.99),
      Transaction(1010, 2017, 1, 31, 200, 99.95),
      Transaction(1011, 2017, 2, 1, 15, 22.00),
      Transaction(1012, 2017, 2, 22, 5, 42.99),
      Transaction(1013, 2017, 2, 22, 100, 42.99),
      Transaction(1014, 2017, 2, 22, 75, 11.99),
      Transaction(1015, 2017, 2, 22, 50, 42.99),
      Transaction(1016, 2017, 2, 22, 200, 99.95)
    )
    val transactionsDS = transactions.toDS()

    println("*** number of partitions: " + transactionsDS.rdd.partitions.size)

    val simpleRoot = exampleRoot + "/Simple"

    transactionsDS.write
      .option("header", "true")
      .csv(simpleRoot)

    println("*** Simple output file count: " +
      countRecursively(new File(simpleRoot), ".csv"))

    val partitionedRoot = exampleRoot + "/Partitioned"

    transactionsDS.write
      .partitionBy("year", "month", "day")
      .option("header", "true")
      .csv(partitionedRoot)

    println("*** Date partitioned output file count: " +
      countRecursively(new File(partitionedRoot), ".csv"))

    val repartitionedRoot = exampleRoot + "/Repartitioned"

    transactionsDS.repartition($"year",$"month",$"day").write
      .partitionBy("year", "month", "day")
      .option("header", "true")
      .csv(repartitionedRoot)

    println("*** Date repartitioned output file count: " +
      countRecursively(new File(repartitionedRoot), ".csv"))

    val sortedDS = transactionsDS.sort($"price")

    val sortedRoot = exampleRoot + "/Sorted"

    println("*** sorted dataset number of partitions: " +
      sortedDS.rdd.partitions.size)

    sortedDS.write
      .partitionBy("year", "month", "day")
      .option("header", "true")
      .csv(sortedRoot)

    println("*** Sorted AND date partitioned output file count: " +
      countRecursively(new File(sortedRoot), ".csv"))

    val sortedRepartitionedRoot = exampleRoot + "/SortedRepartitioned"

    sortedDS.repartition($"year",$"month",$"day").write
      .partitionBy("year", "month", "day")
      .option("header", "true")
      .csv(sortedRepartitionedRoot)

    println("*** Sorted AND date repartitioned output file count: " +
      countRecursively(new File(sortedRepartitionedRoot), ".csv"))

  }

}
