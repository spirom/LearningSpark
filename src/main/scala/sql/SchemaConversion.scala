package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.sql._
import org.apache.spark.sql.types._

case class CC(i: Integer)



object SchemaConversion {
  val spark =
    SparkSession.builder()
      .appName("SQL-SchemaConversion")
      .master("local[4]")
      .getOrCreate()

  def toDataFrame(o: RDD[CC]) : DataFrame = {
    val schema = StructType(
      Seq(StructField("i", IntegerType, true)))
    val rows = o.map(cc => Row(cc.i))
    spark.createDataFrame(rows, schema)
  }

  def main (args: Array[String]) {


    val nums = 1 to 100
    val data = spark.sparkContext.parallelize(nums.map(i => CC(i)), 4)
    val sdata = toDataFrame(data)

    sdata.createOrReplaceTempView("mytable")

    val results = spark.sql("SELECT COUNT(i) FROM mytable WHERE i > 50")

    results.foreach(r => println(r))



  }
}
