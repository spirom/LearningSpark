package hiveql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

// TODO: no longer runs

object LateralViewExplode {
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("HiveQL").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    val transactions = hiveContext.read.json("src/main/resources/data/mixed.json")
    transactions.printSchema()
    transactions.registerTempTable("transactions")

    val data1 = hiveContext.sql("SELECT id, u.oid FROM transactions LATERAL VIEW explode(orders) t AS u")
    data1.schema.printTreeString()
    data1.foreach(println)

    val data2 = hiveContext.sql("SELECT id, u.oid, u.SKU FROM transactions LATERAL VIEW explode(orders) t AS u")
    data2.schema.printTreeString()
    data2.foreach(println)


  }
}
