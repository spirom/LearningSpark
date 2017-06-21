package sql

import org.apache.spark.sql.SparkSession

//
// Since Spark SQL took a while to support the usual syntax for CASE WHEN THEN,
// there has been some confusion. I answered a question on StackOverflow a long
// time ago at https://stackoverflow.com/questions/25157451/spark-sql-case-when-then/25333239
// but didn't write up a persistent example until now.
//
object CaseWhenThen {
  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("SQL-CaseWhenThen")
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

    println("*** A DataFrame has a schema")
    customerDF.printSchema()

    //
    // Register with a table name for SQL queries
    //
    customerDF.createOrReplaceTempView("customer")

    //
    // Spark SQL had a special syntax for this before Spark 1.2.0
    //
    println("*** Syntax before Spark 1.2.0")
    val caseWhen0 =
      spark.sql(
        s"""
           | SELECT IF(id = 1, "One", "NotOne")
           | FROM customer
         """.stripMargin)
    caseWhen0.show()
    caseWhen0.printSchema()


    //
    // Spark 1.2.0 introduced the SQL syntax
    //
    println("*** Syntax starting with Spark 1.2.0")
    val caseWhen1 =
      spark.sql(
        s"""
          | SELECT CASE WHEN id = 1 THEN "One" ELSE "NotOne" END
          | FROM customer
         """.stripMargin)
    caseWhen1.show()
    caseWhen1.printSchema()

    //
    // Like any other result column you can rename it
    //
    println("*** With renaming")
    val caseWhen2 =
      spark.sql(
        s"""
           | SELECT
           |    CASE WHEN id = 1 THEN "One" ELSE "NotOne" END AS IdRedux
           | FROM customer
         """.stripMargin)
    caseWhen2.show()
    caseWhen2.printSchema()

    //
    // There can be boolean expressions in the condition
    //
    println("*** With boolean combination")
    val caseWhen3 =
      spark.sql(
        s"""
           | SELECT
           |    CASE WHEN id = 1 OR id = 2 THEN "OneOrTwo" ELSE "NotOneOrTwo" END AS IdRedux
           | FROM customer
         """.stripMargin)
    caseWhen3.show()
    caseWhen3.printSchema()

    //
    // More than one column can be used in the condition
    //
    println("*** With boolean combination on multiple columns")
    val caseWhen4 =
      spark.sql(
        s"""
           | SELECT
           |    CASE WHEN id = 1 OR state = 'MA' THEN "OneOrMA" ELSE "NotOneOrMA" END AS IdRedux
           | FROM customer
         """.stripMargin)
    caseWhen4.show()
    caseWhen4.printSchema()

    //
    // Conditions can be nested
    //
    println("*** With nested conditions")
    val caseWhen5 =
      spark.sql(
        s"""
           | SELECT
           |    CASE WHEN id = 1 THEN "OneOrMA"
           |    ELSE
           |      CASE WHEN state = 'MA' THEN "OneOrMA" ELSE "NotOneOrMA" END
           |    END AS IdRedux
           | FROM customer
         """.stripMargin)
    caseWhen5.show()
    caseWhen5.printSchema()
  }
}
