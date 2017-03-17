package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, TableScan}

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
case class MyTableScan(count: Int, partitions: Int)
                      (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan
{
  val schema: StructType = StructType(Seq(
    StructField("val", IntegerType, nullable = false),
    StructField("squared", IntegerType, nullable = false),
    StructField("cubed", IntegerType, nullable = false)
  ))

  private def makeRow(i: Int): Row = Row(i, i*i, i*i*i)

  def buildScan: RDD[Row] = {
    val values = (1 to count).map(i => makeRow(i))
    sqlContext.sparkContext.parallelize(values, partitions)
  }

}

//
// Extending RelationProvider allows us to route configuration parameters into
// our implementation -- this is the class we specify below when registering
// temporary tables.
//
class CustomRP extends RelationProvider {

  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    MyTableScan(parameters("rows").toInt,
      parameters("partitions").toInt)(sqlContext)
  }

}

object CustomRelationProvider {
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("SQL-CustomRelationProvider")
        .master("local[4]")
        .getOrCreate()

    // register it as a temporary table to be queried
    // (could register several of these with different parameter values)
    // Note: as of Spark 1.4.0 option names can contain periods or underscores
    spark.sql(
      s"""
        |CREATE TEMPORARY VIEW dataTable
        |USING sql.CustomRP
        |OPTIONS (partitions '9', rows '50')
      """.stripMargin)

    // query the table we registered, using its column names
    val data =
      spark.sql("SELECT * FROM dataTable ORDER BY val")
    data.foreach(r => println(r))
  }

}
