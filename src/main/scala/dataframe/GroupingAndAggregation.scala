package dataframe

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}

//
// Shows various forms of grouping and aggregation.
//
object GroupingAndAggregation {
  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("DataFrame-GroupingAndAggregation")
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

    // groupBy() produces a GroupedData, and you can't do much with
    // one of those other than aggregate it -- you can't even print it

    // basic form of aggregation assigns a function to
    // each non-grouped column -- you map each column you want
    // aggregated to the name of the aggregation function you want
    // to use
    //
    // automatically includes grouping columns in the DataFrame

    println("*** basic form of aggregation")
    customerDF.groupBy("state").agg("discount" -> "max").show()

    // you can turn of grouping columns using the SQL context's
    // configuration properties

    println("*** this time without grouping columns")
    spark.conf.set("spark.sql.retainGroupColumns", "false")
    customerDF.groupBy("state").agg("discount" -> "max").show()

    //
    // When you use $"somestring" to refer to column names, you use the
    // very flexible column-based version of aggregation, allowing you to make
    // full use of the DSL defined in org.apache.spark.sql.functions --
    // this version doesn't automatically include the grouping column
    // in the resulting DataFrame, so you have to add it yourself.
    //

    println("*** Column based aggregation")
    // you can use the Column object to specify aggregation
    customerDF.groupBy("state").agg(max($"discount")).show()

    println("*** Column based aggregation plus grouping columns")
    // but this approach will skip the grouped columns if you don't name them
    customerDF.groupBy("state").agg($"state", max($"discount")).show()

    // Think of this as a user-defined aggregation function -- written in terms
    // of more primitive aggregations
    def stddevFunc(c: Column): Column =
      sqrt(avg(c * c) - (avg(c) * avg(c)))

    println("*** Sort-of a user-defined aggregation function")
    customerDF.groupBy("state").agg($"state", stddevFunc($"discount")).show()

    // there are some special short cuts on GroupedData to aggregate
    // all numeric columns
    println("*** Aggregation short cuts")
    customerDF.groupBy("state").count().show()

  }}
