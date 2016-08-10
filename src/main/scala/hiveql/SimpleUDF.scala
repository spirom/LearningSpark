package hiveql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

// The simple form of user-defined function base class provided in Hive
class DiscountSalesUDF extends org.apache.hadoop.hive.ql.exec.UDF {
  def evaluate (sales: Double, discount: Double) : Double = {
    sales - discount
  }
}

//
// Demonstrate a Hive user-defined function of the simple variety.
//

object SimpleUDF {

  def main (args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("HiveQL-SimpleUDF")
        .master("local[4]")
        .enableHiveSupport()
        .getOrCreate()

    import spark.implicits._

    // create an RDD of tuples with some data
    val custs = Seq(
      (1, "Widget Co", 120000.00, 0.00, "AZ"),
      (2, "Acme Widgets", 410500.00, 500.00, "CA"),
      (3, "Widgetry", 410500.00, 200.00, "CA"),
      (4, "Widgets R Us", 410500.00, 0.0, "CA"),
      (5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    val customerRows = spark.sparkContext.parallelize(custs, 4)
    val customerDF = customerRows.toDF("id", "name", "sales", "discount", "state")

    // register as a temporary table
    customerDF.printSchema()
    customerDF.createOrReplaceTempView("customers")

    // register the UDF with the HiveContext, by referring to the class we defined above --
    // skip the 'TEMPORARY' if you want it to be persisted in the Hive metastore
    spark.sql("CREATE TEMPORARY FUNCTION discounted_sales AS 'hiveql.DiscountSalesUDF'")

    // now use it in a query
    val data1 = spark.sql("SELECT id, discounted_sales(sales, discount) AS sales FROM customers")
    data1.printSchema()
    data1.foreach(r => println(r))

  }
}
