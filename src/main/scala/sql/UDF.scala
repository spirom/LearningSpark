package sql

import org.apache.spark.sql.SQLContext
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
    val conf = new SparkConf().setAppName("UDF").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.createSchemaRDD

    // create an RDD with some data
    val custs = Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    val customerTable = sc.parallelize(custs, 4)

    // DSL usage -- query using a UDF but without SQL

    def westernState(state: String) = Seq("CA", "OR", "WA", "AL").contains(state)

    import sqlContext._
    println("filter using a DSL")
    customerTable.where('state)(westernState).select('id, 'name).foreach(println)

    // for SQL usage  we need to register the table

    sqlContext.registerRDDAsTable(customerTable, "customerTable")

    // WHERE clause

    sqlContext.registerFunction("westernState", westernState _)

    println("UDF in a WHERE")
    val westernStates =
      sqlContext.sql("SELECT * FROM customerTable WHERE westernState(state)")
    westernStates.foreach(println)

    // HAVING clause

    def manyCustomers(cnt: Long) = cnt > 2

    sqlContext.registerFunction("manyCustomers", manyCustomers _)

    println("UDF in a HAVING")
    val statesManyCustomers =
      sqlContext.sql(
        s"""
          |SELECT state, COUNT(id) AS custCount
          |FROM customerTable
          |GROUP BY state
          |HAVING manyCustomers(custCount)
         """.stripMargin)
    statesManyCustomers.foreach(println)

    // GROUP BY clause

    def stateRegion(state:String) = state match {
      case "CA" | "AL" | "OR" | "WA" => "East"
      case "ME" | "NH" | "MA" | "RI" | "CT" | "VT" => "NorthEast"
      case "AZ" | "NM" | "CO" | "UT" => "SouthWest"
    }

    sqlContext.registerFunction("stateRegion", stateRegion _)

    println("UDF in a GROUP BY")
    // note the grouping column repeated since it doesn't have an alias
    val salesByRegion =
      sqlContext.sql(
        s"""
          |SELECT SUM(sales), stateRegion(state) AS totalSales
          |FROM customerTable
          |GROUP BY stateRegion(state)
        """.stripMargin)
    salesByRegion.foreach(println)

    // we can also apply a UDF to the result columns

    def discountRatio(sales: Double, discounts: Double) = discounts/sales

    sqlContext.registerFunction("discountRatio", discountRatio _)

    println("UDF in a result")
    val customerDiscounts =
      sqlContext.sql(
        s"""
          |SELECT id, discountRatio(sales, discounts) AS ratio
          |FROM customerTable
        """.stripMargin)
    customerDiscounts.foreach(println)

    // we can make the UDF create nested structure in the results

    def makeStruct(sales: Double, disc:Double) = SalesDisc(sales, disc)

    sqlContext.registerFunction("makeStruct", makeStruct _)

    println("UDF creating structured result")
    val withStruct =
      sqlContext.sql("SELECT id, sd.sales FROM (SELECT id, makeStruct(sales, discounts) AS sd FROM customerTable) AS d")
    withStruct.foreach(println)
  }

}
