package sql

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

//
// Define data in terms of a case class, convert it to a DataFrame,
// register it as a temporary table, and query it. Print the original DataFrame
// and the ones resulting from the queries, and see their schema.
//
object Basic {
  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("SQL-Basic")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    // create a sequence of case class objects
    // (we defined the case class above)
    val custs = Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    // make it an RDD and convert to a DataFrame
    val customerDF = spark.sparkContext.parallelize(custs, 4).toDF()

    println("*** See the DataFrame contents")
    customerDF.show()

    println("*** See the first few lines of the DataFrame contents")
    customerDF.show(2)

    println("*** Statistics for the numerical columns")
    customerDF.describe("sales", "discount").show()

    println("*** A DataFrame has a schema")
    customerDF.printSchema()

    //
    // Register with a table name for SQL queries
    //
    customerDF.createOrReplaceTempView("customer")

    println("*** Very simple query")
    val allCust = spark.sql("SELECT id, name FROM customer")
    allCust.show()
    println("*** The result has a schema too")
    allCust.printSchema()

    //
    // more complex query: note how it's spread across multiple lines
    //
    println("*** Very simple query with a filter")
    val californiaCust =
      spark.sql(
        s"""
          | SELECT id, name, sales
          | FROM customer
          | WHERE state = 'CA'
         """.stripMargin)
    californiaCust.show()
    californiaCust.printSchema()

    println("*** Queries are case sensitive by default, but this can be disabled")

    spark.conf.set("spark.sql.caseSensitive", "false")
    //
    // the capitalization of "CUSTOMER" here would normally make the query fail
    // with "Table not found"
    //
    val caseInsensitive =
      spark.sql("SELECT * FROM CUSTOMER")
    caseInsensitive.show()
    spark.conf.set("spark.sql.caseSensitive", "true")


  }
}
