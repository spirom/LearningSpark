package sql

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
// to create Parquet files, but can easily be used for other file types.
//
// THie example illustrates how to create and update partitioned tables
// through Spark SQL, using static or dynamic partitioning or a mix of both.
//
object PartitionedTable {

  case class Fact(year: Integer, month: Integer, id: Integer, cat: Integer)

  def main(args: Array[String]) {

    // output goes here
    val exampleRoot = "/tmp/LearningSpark"

    utils.PartitionedTableHierarchy.deleteRecursively(new File(exampleRoot))

    val tableRoot = exampleRoot + "/Table"

    val spark =
      SparkSession.builder()
        .appName("SQL-PartitionedTable")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    // create some sample data
    val ids = 1 to 1200
    val facts = ids.map(id => {
      val month = id % 12 + 1
      val year = 2000 + (month % 12)
      val cat = id % 4
      Fact(year, month, id, cat)
    })


    // make it an RDD and convert to a DataFrame
    val factsDF = spark.sparkContext.parallelize(facts, 4).toDF()

    println("*** Here is some of the sample data")
    factsDF.show(20)

    //
    // Register with a table name for SQL queries
    //
    factsDF.createOrReplaceTempView("original")

    spark.sql(
      s"""
         | DROP TABLE IF EXISTS partitioned
      """.stripMargin)

    //
    // Create the partitioned table, specifying the columns, the file format (Parquet),
    // the partitioning scheme, and where the directory hierarchy and files
    // will be located int he file system. Notice that Spark SQL allows you to
    // specify the partition columns twice, and doesn't require you to make them
    // the last columns.
    //
    spark.sql(
      s"""
         | CREATE TABLE partitioned
         |    (year INTEGER, month INTEGER, id INTEGER, cat INTEGER)
         | USING PARQUET
         | PARTITIONED BY (year, month)
         | LOCATION "$tableRoot"
      """.stripMargin)

    //
    // Now insert the sample data into the partitioned table, relying on
    // dynamic partitioning. Important: notice that the partition columns
    // must be provided last!
    //

    spark.sql(
      s"""
         | INSERT INTO TABLE partitioned
         | SELECT id, cat, year, month FROM original
      """.stripMargin)

    // print the resulting directory hierarchy with the Parquet files

    println("*** partitioned table in the file system, after the initial insert")
    utils.PartitionedTableHierarchy.printRecursively(new File(tableRoot))

    // now we can query the partitioned table

    println("*** query summary of partitioned table")
    val fromPartitioned = spark.sql(
      s"""
         | SELECT year, COUNT(*) as count
         | FROM partitioned
         | GROUP BY year
         | ORDER BY year
      """.stripMargin)

    fromPartitioned.show()

    // dynamic partition insert -- no PARTITION clause

    spark.sql(
      s"""
         | INSERT INTO TABLE partitioned
         | VALUES
         |    (1400, 1, 2016, 1),
         |    (1401, 2, 2017, 3)
      """.stripMargin)

    // dynamic partition insert -- equivalent form with PARTITION clause

    spark.sql(
      s"""
         | INSERT INTO TABLE partitioned
         | PARTITION (year, month)
         | VALUES
         |    (1500, 1, 2016, 2),
         |    (1501, 2, 2017, 4)
      """.stripMargin)

    // static partition insert -- fully specify the partition using the
    // PARTITION clause

    spark.sql(
      s"""
         | INSERT INTO TABLE partitioned
         | PARTITION (year = 2017, month = 7)
         | VALUES
         |    (1600, 1),
         |    (1601, 2)
      """.stripMargin)

    // now for the mixed case -- in the PARTITION clause, 'year' is specified
    // statically and 'month' is dynamic

    spark.sql(
      s"""
         | INSERT INTO TABLE partitioned
         | PARTITION (year = 2017, month)
         | VALUES
         |    (1700, 1, 9),
         |    (1701, 2, 10)
      """.stripMargin)

    // check that all these inserted the data we expected

    println("*** the additional rows that were inserted")

    val afterInserts = spark.sql(
      s"""
         | SELECT year, month, id, cat
         | FROM partitioned
         | WHERE year > 2011
         | ORDER BY year, month
      """.stripMargin)

    afterInserts.show()

    println("*** partitioned table in the file system, after all additional inserts")
    utils.PartitionedTableHierarchy.printRecursively(new File(tableRoot))

    println("*** query summary of partitioned table, after additional inserts")

    val finalCheck = spark.sql(
      s"""
         | SELECT year, COUNT(*) as count
         | FROM partitioned
         | GROUP BY year
         | ORDER BY year
      """.stripMargin)

    finalCheck.show()
  }
}
