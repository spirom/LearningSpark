package dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._

//
// Transform a single column of a DataFrame using a UDF
//
object Transform {

  private case class Cust(id: Integer, name: String, sales: Double, discounts: Double, state: String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataFrame-Transform").setMaster("local[4]")
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

    // the original UDF
    customerDF.show()

    // a trivial UDF
    val myFunc = udf {(x: Double) => x + 1}

    // get the columns, having applied the UDF to the "discount" column and leaving the others as they were
    val colNames = customerDF.columns
    val cols = colNames.map(cName => customerDF.col(cName))
    val mappedCols = cols.map(c => if (c.toString() == "discounts") myFunc(c) else c)

    // use select() to produce the new DataFrame
    val newDF = customerDF.select(mappedCols:_*)
    newDF.show()

  }

}
