package sql

import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.sql._

import scala.collection.immutable

case class OCC(i: Integer)
case class CC(i: Integer, m: Map[Integer,Integer])

// TODO: show how to pull data out of a SchemaRDD, convert to some
// TODO: case class, and back to SchemaRDD again

object SchemaConversion {
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("Parquet").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)



    val nums = 1 to 100
    val data = sc.parallelize(nums.map(i => CC(i, new immutable.HashMap[Integer, Integer]())), 4)
    val sdata = sqlContext.createSchemaRDD(data)
    sdata.foreach(println)



  }
}
