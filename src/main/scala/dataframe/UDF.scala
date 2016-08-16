package dataframe

import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable

// needed for registering UDFs
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.functions.struct


object UDF {
  private case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("DataFrame-UDF")
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
    val customerDF = spark.sparkContext.parallelize(custs, 4).toDF()

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

    //
    // literals in UDF calls
    // see: http://stackoverflow.com/questions/29406913/how-to-use-constant-value-in-udf-of-spark-sqldataframe
    //
    // In order to pass a literal to a UDF you need to create a literal column
    // using org.apache.spark.sql.functions.lit().
    //

    val salesFilter = udf {(s: Double, min: Double) => s > min}

    println("*** UDF with scalar constant parameter")
    customerDF.filter(salesFilter($"sales", lit(2000.0))).show()

    //
    // You can create an array of literals using
    // org.apache.spark.sql.functions.array(). This results in the UDF being
    // passed a scala.collection.mutable.WrappedArray.
    //

    val stateFilter =
      udf {(state:String, regionStates: mutable.WrappedArray[String]) =>
        regionStates.contains(state)
      }

    println("*** UDF with array constant parameter")
    customerDF.filter(stateFilter($"state",
      array(lit("CA"), lit("MA"), lit("NY"), lit("NJ")))).show()

    //
    // You can create a struct of literals using
    // org.apache.spark.sql.functions.struct(). This results in the UDF being
    // passed something you an access as a org.apache.spark.sql.Row.
    //
    // This example UDF is based on knowing the length and type of the struct
    // but of course it could instead use thestruct.schema to figure it
    // out at runtime.
    //

    val multipleFilter = udf { (state: String, discount: Double,
                                thestruct: Row) =>
      state == thestruct.getString(0) && discount > thestruct.getDouble(1) }

    println("*** UDF with array constant parameter")
    customerDF.filter(
      multipleFilter($"state", $"discount", struct(lit("CA"), lit(100.0)))
    ).show()
  }
}
