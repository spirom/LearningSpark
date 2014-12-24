package sql

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.sources._

import scala.collection.mutable.{HashMap, ListBuffer}
import scala.collection.parallel.mutable

// TODO: reorganize to look like external DB?
// TODO: don't _completely_ filter the rows, to show its not necessary

//
// Demonstrate the Spark SQL external data source API, but for
// simplicity don't connect to an external system -- just create a
// synthetic table whose size and partitioning are determined by
// configuration parameters.
//

class MegaFilter(filters: Array[Filter]) {

  val attrToFilters: Map[String, Array[Filter]] = filters
    .map(f => (MegaFilter.getFilterAttribute(f), f))
    .groupBy(attrFilter => attrFilter._1)
    .mapValues(a => a.map(p => p._2))

  def apply(attr: String, v: Int): Boolean = {
    val filters = attrToFilters.getOrElse(attr, new Array[Filter](0))
    MegaFilter.satisfiesAll(v, filters)
  }

  def apply(r: HashMap[String, Int]): Boolean = {
    r.forall({
      case (attr, v) => {
        val filters = attrToFilters.getOrElse(attr, new Array[Filter](0))
        MegaFilter.satisfiesAll(v, filters)
      }
    })
  }

}

object MegaFilter {
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

  def satisfiesAll(value: Int, filters: Array[Filter]): Boolean = {
    filters.forall({
      case EqualTo(attr, v) => value == v.asInstanceOf[Int]
      case GreaterThan(attr, v) => value > v.asInstanceOf[Int]
      case LessThan(attr, v) => value < v.asInstanceOf[Int]
      case GreaterThanOrEqual(attr, v) => value >= v.asInstanceOf[Int]
      case LessThanOrEqual(attr, v) => value <= v.asInstanceOf[Int]
      case In(attr, vs) => vs.exists(v => value == v.asInstanceOf[Int])
    })
  }
}

//
// Extending TableScan allows us to describe the schema and
// provide the rows when requested
//
case class MyPFTableScan(count: Int, partitions: Int)
                      (@transient val sqlContext: SQLContext)
  extends PrunedFilteredScan {

  val schema: StructType = StructType(Seq(
    StructField("val", IntegerType, nullable = false),
    StructField("squared", IntegerType, nullable = false),
    StructField("cubed", IntegerType, nullable = false)
  ))

  // NOTE: it's not enough to merely produce the right columns:
  // they must be produced in the order requested, which may be different
  // from the order specified when returning the schema
  private def makeRow(i: Int, requiredColumns: Array[String]): HashMap[String, Int] = {
    val m = new HashMap[String, Int]()
    if (requiredColumns.contains("val")) m += ("val" -> i)
    if (requiredColumns.contains("squared")) m += ("squared" -> i*i)
    if (requiredColumns.contains("cubed")) m += ("cubed" -> i*i*i)
    m
  }

  // get the columns in the right order and wrap up as a Row
  private def wrapRow(m: HashMap[String, Int], requiredColumns: Array[String]): Row = {
    val l = requiredColumns.map(c => m(c))
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
    val megaFilter = new MegaFilter(filters)
    val keys = (1 to count).filter(v => megaFilter.apply("val", v))
    val rows = keys
      .map(i => makeRow(i, requiredColumns))
      .filter(r => megaFilter.apply(r))
      .map(r => wrapRow(r, requiredColumns))
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
