package dataframe

import org.apache.spark.sql.SQLContext
// needed for registering UDFs
import org.apache.spark.sql.functions.udf
import org.apache.spark.{SparkContext, SparkConf}

object UDF {
  private case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataFrame-UDF").setMaster("local[4]")
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

    // use UDF to construct Column from other Columns

    val mySales = udf {(sales:Double, disc:Double) => sales - disc}

    println("*** UDF used for selecting")
    customerDF.select($"id",
      mySales($"sales", $"discount").as("After Discount")).show()

    // UDF filter

    val myNameFilter = udf {(s: String) => s.startsWith("W")}

    println("*** UDF used for filtering")
    customerDF.filter(myNameFilter($"name")).show()

    // UDF grouping

    def stateRegion = udf {
      (state: String) => state match {
        case "CA" | "AK" | "OR" | "WA" => "West"
        case "ME" | "NH" | "MA" | "RI" | "CT" | "VT" => "NorthEast"
        case "AZ" | "NM" | "CO" | "UT" => "SouthWest"
      }
    }

    println("*** UDF used for grouping")
    customerDF.groupBy(stateRegion($"state").as("Region")).count().show()

    // UDF sorting

    println("*** UDF used for sorting")
    customerDF.orderBy(stateRegion($"state")).orderBy(mySales($"sales", $"discount")).show()
  }
}
