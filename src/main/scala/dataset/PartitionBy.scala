package dataset

import java.io.File

import java.io.File
import org.apache.spark.sql.SparkSession


//
// A common practice for organizing large amounts of data is to build a
// directory hierarchy that represents the overall structure of the data, and
// shard the data into files at the leaf nodes of this hierarchy. This is
// most commonly done with dates, where one level of the hierarchy is divided
// by year, the next by month, then by day, etc. It is also commonly done with
// locations and categories, and of course these can be combined into a single
// hierarchy.
//
// This practice is so common in big data that it is incorporated into various
// Hadoop and Spark features, to make it easy to create such hierarchies and
// also to efficiently and easily query them. It is now very commonly used
// to create Parquet files, but here to make the example more transparent (you
// can easily look inside each file) we use it to generate a series of CSV files.
//
// This example organizes a series of transaction records into a year/month
// folder hierarchy. A common failure mode in generating output this way from
// Spark is to end up with more files than you expect -- sometimes by a huge
// factor. This can maye your data harder to curate, increase your storage costs
// and decrease the efficiency of your subsequent queries. The problem is that
// the most efficient way to generate the output is to write a separate file
// for each partition. IF your DataSet isn't already partitioned by the fields
// you use to create the folder hierarchy, writing it out will introduce
// additional partitioning, creating an explosion of output files.
//
// The problem is made worse by the name of Spark SQL's approach to writing
// a DataSet into a folder hierarchy -- partitionBy() -- which partitions the
// output but DOES NOT repartition the DataSet beforehand. To control the
// number of output files, you need to first call repartition() on the
// DataSet using at least the fields that you use for the folder hierarchy.
//
object PartitionBy {

  case class Transaction(id: Long, year: Int, month: Int, day: Int,
                         quantity: Long, price: Double)

  def main(args: Array[String]) {

    // output goes here
    val exampleRoot = "/tmp/LearningSpark"

    val spark =
      SparkSession.builder()
        .appName("Dataset-PartitionBy")
        .master("local[4]")
        // set the parallelism to emphasize the impact of partitioning
        .config("spark.default.parallelism", 12)
        .getOrCreate()

    utils.PartitionedTableHierarchy.deleteRecursively(new File(exampleRoot))

    import spark.implicits._

    // 24 transactions
    val transactions = Seq(
      // 2016-11-05
      Transaction(1001, 2016, 11, 5, 100, 42.99),
      Transaction(1002, 2016, 11, 5, 75, 42.99),
      // 2016-11-15
      Transaction(1003, 2016, 11, 15, 50, 75.95),
      Transaction(1004, 2016, 11, 15, 50, 19.95),
      Transaction(1005, 2016, 11, 15, 25, 42.99),
      // 2016-12-11
      Transaction(1006, 2016, 12, 11, 22, 11.00),
      Transaction(1007, 2016, 12, 11, 100, 170.00),
      Transaction(1008, 2016, 12, 11, 50, 5.99),
      Transaction(1009, 2016, 12, 11, 10, 11.00),
      // 2016-12-22
      Transaction(1010, 2016, 12, 22, 20, 10.99),
      Transaction(1011, 2016, 12, 22, 10, 75.95),
      // 2017-01-01
      Transaction(1012, 2017, 1, 2, 1020, 9.99),
      Transaction(1013, 2017, 1, 2, 100, 19.99),
      // 2017-01-31
      Transaction(1014, 2017, 1, 31, 200, 99.95),
      Transaction(1015, 2017, 1, 31, 80, 75.95),
      Transaction(1016, 2017, 1, 31, 200, 100.95),
      // 2017-02-01
      Transaction(1017, 2017, 2, 1, 15, 22.00),
      Transaction(1018, 2017, 2, 1, 100, 75.95),
      Transaction(1019, 2017, 2, 1, 5, 22.00),
      // 2017-02-22
      Transaction(1020, 2017, 2, 22, 5, 42.99),
      Transaction(1021, 2017, 2, 22, 100, 42.99),
      Transaction(1022, 2017, 2, 22, 75, 11.99),
      Transaction(1023, 2017, 2, 22, 50, 42.99),
      Transaction(1024, 2017, 2, 22, 200, 99.95)
    )
    val transactionsDS = transactions.toDS()

    // the number of partitions comes from the default parallelism
    println("*** number of partitions: " + transactionsDS.rdd.partitions.size)

    //
    // First let's write this DataSet out in CSV form without any directory
    // hierarchy -- this is rather sobering as we end up with one file for
    // each partition, which could be an awful lot if you're trying to make
    // use of a large cluster, but it may be useful to achieve faster reads
    //

    val simpleRoot = exampleRoot + "/Simple"

    transactionsDS.write
      .option("header", "true")
      .csv(simpleRoot)

    println("*** Simple output file count: " +
      utils.PartitionedTableHierarchy.countRecursively(new File(simpleRoot), ".csv"))

    utils.PartitionedTableHierarchy. printRecursively(new File(simpleRoot))


    //
    // This time we'll specify a year/month folder hierarchy. We end up with
    // even more files because some months have data in more than one partition.
    // If the DataSet was larger and the number of partitions was larger, the
    // explosion in the number of files could be even more dramatic.
    //
    // NOTE: In the worst case the number of files you end up here can be the
    // number of leaf directories MULTIPLIED BY the number of partitions.
    //

    val partitionedRoot = exampleRoot + "/Partitioned"

    transactionsDS.write
      .partitionBy("year", "month")
      .option("header", "true")
      .csv(partitionedRoot)

    println("*** Date partitioned output file count: " +
      utils.PartitionedTableHierarchy.countRecursively(new File(partitionedRoot), ".csv"))

    utils.PartitionedTableHierarchy.printRecursively(new File(partitionedRoot))

    //
    // Now we'll repartition the DataSet before writing it out. You have some
    // flexibility in HOW you repartition the DataSet, and you may need this
    // if it is huge. The basic idea is to exercise control on how many
    // partitions you want at each leaf node of the hierarchy -- it doesn't
    // have to be just one, but that's what we'll use by simply partitioning on
    // year and month.
    //

    val repartitionedRoot = exampleRoot + "/Repartitioned"

    transactionsDS.repartition($"year",$"month").write
      .partitionBy("year", "month")
      .option("header", "true")
      .csv(repartitionedRoot)

    println("*** Date repartitioned output file count: " +
      utils.PartitionedTableHierarchy.countRecursively(new File(repartitionedRoot), ".csv"))

    utils.PartitionedTableHierarchy.printRecursively(new File(repartitionedRoot))

    //
    // Now we'll read the data back from the hierarchy of CSV files. Notice
    // we don't have to navigate the hierarchy: Spark SQL does that for us
    // once we specify the base path. Notice that year and month appear as
    // columns, tacked onto the end -- that's because they don't actually
    // appear in the individual CSV files.
    //

    val allDF = spark
      .read
      .option("basePath", partitionedRoot)
      .option("header", "true")
      .csv(partitionedRoot)

    allDF.show()

    //
    // We can demonstrate the fact that year and month do not appear in the CSV
    // files by looking at the individual files directly, but that's no fun.
    // Here we use a "dirty trick" to reach down to a single month and
    // read its CSV file(s): no years or months!
    //

    val oneDF = spark
      .read
      .option("basePath", partitionedRoot + "/year=2016/month=11")
      .option("header", "true")
      .csv(partitionedRoot + "/year=2016/month=11")

    oneDF.show()

    //
    // But what if just wanted to read one month? THat's easy using path magic:
    // simply undo our 'basePath' "dirty trick". Now the year and month
    // are back.
    //

    val oneMonth = spark
      .read
      .option("basePath", partitionedRoot)
      .option("header", "true")
      .csv(partitionedRoot + "/year=2016/month=11")

    oneMonth.show()

    //
    // But there's a better way: this is after all Spark SQL -- we just use
    // filtering, and the Spark SQL optimizer will even push the filter down so
    // that only the relevant files will be read.
    //

    val twoMonthQuery = spark
      .read
      .option("basePath", partitionedRoot)
      .option("header", "true")
      .csv(partitionedRoot)
      .filter("year = 2016")

    twoMonthQuery.show()


  }

}
