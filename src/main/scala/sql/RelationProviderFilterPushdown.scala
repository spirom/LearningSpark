package sql

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.sources._

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
  extends PrunedFilteredScan {
  private val attributes = Seq("val", "squared", "cubed")

  private def getFilterAttribute(f: Filter): String = {
    f match {
      case EqualTo(attr, v) => attr
      case GreaterThan(attr, v) => attr
      case LessThan(attr, v) => attr
      case GreaterThanOrEqual(attr, v) => attr
      case LessThanOrEqual(attr, v) => attr
      case In(attr, vs) => attr
    }
  }

  private def satisfiesAllFilters(value: Int, filters: Array[Filter]): Boolean = {
    filters.forall({
      case EqualTo(attr, v) => value == v.asInstanceOf[Int]
      case GreaterThan(attr, v) => v.asInstanceOf[Int] > value
      case LessThan(attr, v) => v.asInstanceOf[Int] < value
      case GreaterThanOrEqual(attr, v) => v.asInstanceOf[Int] >= value
      case LessThanOrEqual(attr, v) => v.asInstanceOf[Int] <= value
      case In(attr, vs) => vs.exists(v => v.asInstanceOf[Int] == value)
    })
  }

  private def splitAttributes(filters: Array[Filter], names: Seq[String]):
    Map[String, Array[Filter]] =
  {
    val attrToFilters = filters
      .map(f => (getFilterAttribute(f), f))
      .groupBy(attrFilter => attrFilter._1)
      .mapValues(a => a.map(p => p._2))
    attrToFilters
  }

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

  // NOTE: you're not actually guaranteed to get filters -- in particular
  // you won't get them if the filter you specified can't be turned into a
  // conjunction fo simple filters -- you might expect to get SOME filters in
  // such cases, but it seems you won't
  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // get the filters organized by the full set of attributes
    // (could probably use requiredColumns as Spark SQL is likely to ask for
    // all columns the query filters on, just so it can reapply the filters itself)
    val attrToFilters = splitAttributes(filters, attributes)
    val keys = (1 to count).filter(v => satisfiesAllFilters(v, attrToFilters("val")))
    val rows = keys.map(i => makeRow(i, requiredColumns))
    sqlContext.sparkContext.parallelize(rows, partitions)
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
    // NOTE: requests the columns val, cubed, squared in that order!
    val data =
      sqlContext.sql(
        s"""
          |SELECT val, cubed
          |FROM dataTable
          |WHERE val <= 40 AND squared >= 900
          |ORDER BY val
        """.stripMargin)
    data.foreach(println)
  }

}
