package sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.sources._

import scala.collection.mutable.{ArrayBuffer, HashMap}

//
// Demonstrate the Spark SQL external data source API, but for
// simplicity don't connect to an external system -- just create a
// synthetic table whose size and partitioning are determined by
// configuration parameters.
//

//
// Simulate a database with an impoverished query language. A single table with
// a fixed number of rows, and each row has an
// integer key, and two more fields containing the key squared and cubed,
// respectively. The only kind of query supported is an inclusive range
// query on the key.
//

case class RangeDBRecord(key: Int, squared: Int, cubed: Int)

class RangeIterator(begin: Int, end: Int) extends Iterator[RangeDBRecord] {
  var pos: Int = begin

  def hasNext: Boolean = pos <= end

  def next(): RangeDBRecord = {
    val rec = RangeDBRecord(pos, pos*pos, pos*pos*pos)
    pos = pos + 1
    rec
  }
}

class RangeDB(numRecords: Int) {

  def getRecords(min: Option[Int], max: Option[Int]): RangeIterator = {
    new RangeIterator(min.getOrElse(1), max.getOrElse(numRecords))
  }
}

//
// The FilterInterpreter class contains everything
// we need to interpret the filter language, both for constructing the query to
// the back-end data engine and for filtering the rows it gives us.
//

class FilterInterpreter(allFilters: Array[Filter]) {

  //
  // First organize the filters into a map according to the columns they apply to
  //
  private val allAttrToFilters: Map[String, Array[Filter]] = allFilters
    .map(f => (getFilterAttribute(f), f))
    .groupBy(attrFilter => attrFilter._1)
    .mapValues(a => a.map(p => p._2))

  //
  // pull out the parts of the filters we'll push out to the back-and data engine
  //
  val (min, max, otherKeyFilters) = splitKeyFilter

  //
  // and tidy up the remaining filters that we'll apply to records returned
  //
  private val attrToFilters = allAttrToFilters - "val" + ("val" -> otherKeyFilters)

  //
  // Apply the filters to a returned record
  //
  def apply(r: Map[String, Int]): Boolean = {
    r.forall({
      case (attr, v) => {
        val filters = attrToFilters.getOrElse(attr, new Array[Filter](0))
        satisfiesAll(v, filters)
      }
    })
  }

  private def splitKeyFilter: (Option[Int], Option[Int], Array[Filter]) = {
    val keyFilters = allAttrToFilters.getOrElse("val", new Array[Filter](0))
    var min: Option[Int] = None
    var max: Option[Int] = None
    val others = new ArrayBuffer[Filter](0)
    keyFilters.foreach({
      case GreaterThan(attr, v) => min = Some(v.asInstanceOf[Int] + 1)
      case LessThan(attr, v) => max = Some(v.asInstanceOf[Int] - 1)
      case GreaterThanOrEqual(attr, v) => min = Some(v.asInstanceOf[Int])
      case LessThanOrEqual(attr, v) => max = Some(v.asInstanceOf[Int])
      case _ => others.++=: _
    })
    (min, max, others.toArray)
  }

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

  private def satisfiesAll(value: Int, filters: Array[Filter]): Boolean = {
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
  extends BaseRelation with PrunedFilteredScan {

  // instantiate the (fake) back-end storage engine
  val db = new RangeDB(count)

  val schema: StructType = StructType(Seq(
    StructField("val", IntegerType, nullable = false),
    StructField("squared", IntegerType, nullable = false),
    StructField("cubed", IntegerType, nullable = false)
  ))

  // massage a back-end row into a map for uniformity
  private def makeMap(rec: RangeDBRecord): Map[String, Int] = {
    val m = new HashMap[String, Int]()
    m += ("val" -> rec.key)
    m += ("squared" -> rec.squared)
    m += ("cubed" -> rec.cubed)
    m.toMap
  }

  // project down to the required columns in the right order and wrap up as a Row
  private def projectAndWrapRow(m: Map[String, Int],
                                requiredColumns: Array[String]): Row = {
    val l = requiredColumns.map(c => m(c))
    val r = Row.fromSeq(l)
    r
  }

  // Get the data, filter and project it, and return as an RDD
  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // organize the filters
    val filterInterpreter = new FilterInterpreter(filters)
    // get the data, pushing as much filtering to the back-end as possible
    // (in this case, not much)
    val rowIterator = db.getRecords(filterInterpreter.min, filterInterpreter.max)
    val rows = rowIterator
      .map(rec => makeMap(rec))
      .filter(r => filterInterpreter.apply(r))
      .map(r => projectAndWrapRow(r, requiredColumns))
    sqlContext.sparkContext.parallelize(rows.toSeq, partitions)
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
    val spark =
      SparkSession.builder()
        .appName("SQL-RelationProviderFilterPushdown")
        .master("local[4]")
        .getOrCreate()

    // register it as a temporary table to be queried
    // (could register several of these with different parameter values)
    spark.sql(
      s"""
        |CREATE TEMPORARY VIEW dataTable
        |USING sql.CustomPFRP
        |OPTIONS (partitions '9', rows '50')
      """.stripMargin)

    // query the table we registered, using its column names
    // NOTE: requests the columns val, cubed, squared in that order!
    val data =
      spark.sql(
        s"""
          |SELECT val, cubed
          |FROM dataTable
          |WHERE val <= 40 AND squared >= 900
          |ORDER BY val
        """.stripMargin)
    data.foreach(r => println(r))
  }

}
