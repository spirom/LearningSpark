package dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._

object Select {

  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataFrame-Basic").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    // create an RDD with some data
    val custs = Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    val customerDF = sc.parallelize(custs, 4).toDF()

    println("*** use * to select() all columns")

    customerDF.select("*").show()

    println("*** select multiple columns")

    customerDF.select("id", "discount").show()

    println("*** use apply() on DataFrame to create column objects, and select though them")

    customerDF.select(customerDF("id"), customerDF("discount")).show()

    println("*** use as() on Column to rename")

    customerDF.select(customerDF("id").as("Customer ID"),
                      customerDF("discount").as("Total Discount")).show()

    println("*** $ as shorthand to obtain Column")

    customerDF.select($"id".as("Customer ID"), $"discount".as("Total Discount")).show()

    println("*** use DSL to manipulate values")

    customerDF.select(($"discount" * 2).as("Double Discount")).show()

    customerDF.select(
      ($"sales" - $"discount").as("After Discount")).show()

    println("*** use * to select() all columns and add more")

    customerDF.select(customerDF("*"), $"id".as("newID")).show()
  }
}
