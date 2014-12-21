package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.sql.sources.{TableScan, RelationProvider}

case class CustomRelation(partitions: Int)(@transient val sqlContext: SQLContext) extends TableScan {
  val schema = StructType(Seq(
    StructField("val", IntegerType, nullable = false),
    StructField("squared", IntegerType, nullable = false),
    StructField("cubed", IntegerType, nullable = false)
  ))

  private def makeRow(i: Int): Row = Row(i, i*i, i*i*i)

  def buildScan: RDD[Row] = {
    val values = (1 to 100).map(i => makeRow(i))
    sqlContext.sparkContext.parallelize(values, partitions)
  }
}

class CustomRP extends RelationProvider {

  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    CustomRelation(parameters("partitions").toInt)(sqlContext)
  }
}

object CustomRelationProvider {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RelationProvider").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    sqlContext.sql(
      s"""
        |CREATE TEMPORARY TABLE dataTable
        |USING sql.CustomRP
        |OPTIONS (partitions '9')
      """.stripMargin)

    val data =
      sqlContext.sql("SELECT * FROM dataTable ORDER BY val")
    data.foreach(println)
  }

}
