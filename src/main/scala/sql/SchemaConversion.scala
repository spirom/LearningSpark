package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.sql._
import org.apache.spark.sql.types._

case class CC(i: Integer)



object SchemaConversion {
  val conf = new SparkConf().setAppName("SchemaConversion").setMaster("local[4]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  def toDataFrame(o: RDD[CC]) : DataFrame = {
    val schema = StructType(
      Seq(StructField("i", IntegerType, true)))
    val rows = o.map(cc => Row(cc.i))
    sqlContext.createDataFrame(rows, schema)
  }

  def main (args: Array[String]) {


    val nums = 1 to 100
    val data = sc.parallelize(nums.map(i => CC(i)), 4)
    val sdata = toDataFrame(data)

    sdata.registerTempTable("mytable")

    val results = sqlContext.sql("SELECT COUNT(i) FROM mytable WHERE i > 50")

    results.foreach(println)



  }
}
