package hiveql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

// TODO: no longer runs

object LateralViewExplode {
  def main (args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("HiveQL-LateralViewExplode")
        .master("local[4]")
        .enableHiveSupport()
        .getOrCreate()

    val transactions = spark.read.json("src/main/resources/data/mixed.json")
    transactions.printSchema()
    transactions.createOrReplaceTempView("transactions")

    val data1 = spark.sql("SELECT id, u.oid FROM transactions LATERAL VIEW explode(orders) t AS u")
    data1.schema.printTreeString()
    data1.foreach(r => println(r))

    val data2 = spark.sql("SELECT id, u.oid, u.SKU FROM transactions LATERAL VIEW explode(orders) t AS u")
    data2.schema.printTreeString()
    data2.foreach(r => println(r))

  }
}
