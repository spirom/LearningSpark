package dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._

object Basic {
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

    println("*** toString() just gives you the schema")

    println(customerDF.toString())

    println("*** It's better to use printSchema()")

    customerDF.printSchema()

    println("*** show() gives you neatly formatted data")

    customerDF.show()

    println("*** use select() to choose one column")

    customerDF.select("id").show()

    println("*** use select() for multiple columns")

    customerDF.select("sales", "state").show()

    println("*** use filter() to choose rows")

    customerDF.filter($"state".equalTo("CA")).show()

    // groupBy() produces a GroupedData, and you can't do much with
    // one of those other than aggregate it -- you can't even print it

    // most general form of aggregation assigns a function to
    // each non-grouped column

    println("*** general form of aggregation")
    customerDF.groupBy("state").agg("discount" -> "max").show()

    println("*** Column based aggregation")
    // you can use the Column object to specify aggregation
    customerDF.groupBy("state").agg(max($"discount")).show()

    println("*** Column based aggregation plus grouping columns")
    // but this approach will skip the grouped columns if you don't name them
    customerDF.groupBy("state").agg($"state", max($"discount")).show()

    // there are some special short cuts on GroupedData to aggregate
    // all numeric columns
    println("*** Aggregation short cuts")
    customerDF.groupBy("state").count().show()

  }}
