package experiments

import java.io.File

import org.apache.spark.sql.SparkSession


object PartitionedTableColumnOrder {

  case class Fact(year: Integer, month: Integer, id: Integer, cat: Integer)

  def main(args: Array[String]) {

    // output goes here
    val exampleRoot = "/tmp/LearningSpark"

    utils.PartitionedTableHierarchy.deleteRecursively(new File(exampleRoot))

    val tableRoot = exampleRoot + "/Table"

    val spark =
      SparkSession.builder()
        .appName("SQL-PartitionedTableColumnOrder")
        .master("local[4]")
        .getOrCreate()


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

    // dynamic partition insert -- no PARTITION clause

    spark.sql(
      s"""
         | INSERT INTO partitioned PARTITION (year, month)
         | VALUES
         |    (1400, 1, 2016, 1),
         |    (1401, 2, 2017, 3)
      """.stripMargin)


    println("*** the rows that were inserted")

    val afterInserts = spark.sql(
      s"""
         | SELECT year, month, id, cat
         | FROM partitioned
         | ORDER BY year, month
      """.stripMargin)

    afterInserts.show()



  }
}
