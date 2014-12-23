package sql

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.sources.{Filter, PrunedFilteredScan, RelationProvider, TableScan}

import scala.collection.mutable.ListBuffer

// TODO: produce columns in the right order
// TODO: filter the rows
// TODO: don't _completely_ filter the rows, to show its not necessary

//
// Demonstrate the Spark SQL external data source API, but for
// simplicity don't connect to an external system -- just create a
// synthetic table whose size and partitioning are determined by
// configuration parameters.
//

//
// Extending TableScan allows us to describe the schema and
// provide the rows when requested
//
case class MyPFTableScan(count: Int, partitions: Int)
                      (@transient val sqlContext: SQLContext)
  extends PrunedFilteredScan
{
  val schema: StructType = StructType(Seq(
    StructField("val", IntegerType, nullable = false),
    StructField("squared", IntegerType, nullable = false),
    StructField("cubed", IntegerType, nullable = false)
  ))

  // NOTE: it's not enough to merely produce the right columns:
  // they must be produced in the order requested, which may be different
  // from the order specified when returning the schema
  private def makeRow(i: Int, requiredColumns: Array[String]): Row = {
    val l = ListBuffer[Int]()
    if (requiredColumns.contains("val")) l.append(i)

    if (requiredColumns.contains("cubed")) l.append(i*i*i)
    if (requiredColumns.contains("squared")) l.append(i*i)
    val r = Row.fromSeq(l)
    r
  }

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val values = (1 to count).map(i => makeRow(i, requiredColumns))
    sqlContext.sparkContext.parallelize(values, partitions)
  }

}

//
// Extending RelationProvider allows us to route configuration parameters into
// our implementation -- this is the class we specify below when registering
// temporary tables.
//
class CustomPFRP extends RelationProvider {

  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    MyPFTableScan(parameters("rows").toInt,
      parameters("partitions").toInt)(sqlContext)
  }

}

object RelationProviderFilterPushdown {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RelationProvider").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // register it as a temporary table to be queried
    // (could register several of these with different parameter values)
    sqlContext.sql(
      s"""
        |CREATE TEMPORARY TABLE dataTable
        |USING sql.CustomPFRP
        |OPTIONS (partitions '9', rows '50')
      """.stripMargin)

    // query the table we registered, using its column names
    val data =
      sqlContext.sql(
        s"""
          |SELECT val, cubed, val
          |FROM dataTable
          |WHERE val <= 10 OR squared >= 900
          |ORDER BY val
        """.stripMargin)
    data.foreach(println)
  }

}
