package dataframe

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

//
// Here we create a DataFrame from an RDD[Row] and a synthetic schema.
// It's the same example as used in Basic.scala, but there the DataFrame is
// constructed from a sequence of case class objects.
//
object FromRowsAndSchema {
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("DataFrame-DropDuplicates")
        .master("local[4]")
        .getOrCreate()

    // create an RDD of Rows with some data
    val custs = Seq(
      Row(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Row(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Row(3, "Widgetry", 410500.00, 200.00, "CA"),
      Row(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Row(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    val customerRows = spark.sparkContext.parallelize(custs, 4)

    val customerSchema = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("sales", DoubleType, true),
        StructField("discount", DoubleType, true),
        StructField("state", StringType, true)
      )
    )

    // put the RDD[Row] and schema together to make a DataFrame
    val customerDF = spark.createDataFrame(customerRows, customerSchema)

    customerDF.printSchema()

    customerDF.show()

  }
}
