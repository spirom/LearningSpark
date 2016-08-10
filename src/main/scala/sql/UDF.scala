package sql

import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.{SparkContext, SparkConf}

// a case class for our sample table
case class Cust(id: Integer, name: String, sales: Double, discounts: Double, state: String)

// an extra case class to show how UDFs can generate structure
case class SalesDisc(sales: Double, discounts: Double)

//
// Show various ways to query in SQL using user-defined functions UDFs.
//

object UDF {

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("SQL-UDF")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    // create an RDD with some data
    val custs = Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    val customerTable = spark.sparkContext.parallelize(custs, 4).toDF()

    // DSL usage -- query using a UDF but without SQL
    // (this example has been repalced by the one in dataframe.UDF)

    def westernState(state: String) = Seq("CA", "OR", "WA", "AK").contains(state)

    // for SQL usage  we need to register the table

    customerTable.createOrReplaceTempView("customerTable")

    // WHERE clause

    spark.udf.register("westernState", westernState _)

    println("UDF in a WHERE")
    val westernStates =
      spark.sql("SELECT * FROM customerTable WHERE westernState(state)")
    westernStates.foreach(r => println(r))

    // HAVING clause

    def manyCustomers(cnt: Long) = cnt > 2

    spark.udf.register("manyCustomers", manyCustomers _)

    println("UDF in a HAVING")
    val statesManyCustomers =
      spark.sql(
        s"""
          |SELECT state, COUNT(id) AS custCount
          |FROM customerTable
          |GROUP BY state
          |HAVING manyCustomers(custCount)
         """.stripMargin)
    statesManyCustomers.foreach(r => println(println(r)))

    // GROUP BY clause

    def stateRegion(state:String) = state match {
      case "CA" | "AK" | "OR" | "WA" => "West"
      case "ME" | "NH" | "MA" | "RI" | "CT" | "VT" => "NorthEast"
      case "AZ" | "NM" | "CO" | "UT" => "SouthWest"
    }

    spark.udf.register("stateRegion", stateRegion _)

    println("UDF in a GROUP BY")
    // note the grouping column repeated since it doesn't have an alias
    val salesByRegion =
      spark.sql(
        s"""
          |SELECT SUM(sales), stateRegion(state) AS totalSales
          |FROM customerTable
          |GROUP BY stateRegion(state)
        """.stripMargin)
    salesByRegion.foreach(r => println(r))

    // we can also apply a UDF to the result columns

    def discountRatio(sales: Double, discounts: Double) = discounts/sales

    spark.udf.register("discountRatio", discountRatio _)

    println("UDF in a result")
    val customerDiscounts =
      spark.sql(
        s"""
          |SELECT id, discountRatio(sales, discounts) AS ratio
          |FROM customerTable
        """.stripMargin)
    customerDiscounts.foreach(r => println(r))

    // we can make the UDF create nested structure in the results


    def makeStruct(sales: Double, disc:Double) = SalesDisc(sales, disc)

    spark.udf.register("makeStruct", makeStruct _)

    // these failed in Spark 1.3.0 -- reported SPARK-6054 -- but work again in 1.3.1

    println("UDF creating structured result")
    val withStruct =
      spark.sql("SELECT makeStruct(sales, discounts) AS sd FROM customerTable")
    withStruct.foreach(r => println(r))

    println("UDF with nested query creating structured result")
    val nestedStruct =
      spark.sql("SELECT id, sd.sales FROM (SELECT id, makeStruct(sales, discounts) AS sd FROM customerTable) AS d")
    nestedStruct.foreach(r => println(r))
  }

}
