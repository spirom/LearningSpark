package dataframe

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

//
// Explore interoperability between DataFrame and Dataset. Note that Dataset
// is covered in much greater detail in the 'dataset' directory.
//
object DatasetConversion {

  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  case class StateSales(state: String, sales: Double)

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("DataFrame-DatasetConversion")
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

    // Create the DataFrame without passing through an RDD
    val customerDF : DataFrame = spark.createDataFrame(custs)

    println("*** DataFrame schema")

    customerDF.printSchema()

    println("*** DataFrame contents")

    customerDF.show()

    println("*** Select and filter the DataFrame")

    val smallerDF =
      customerDF.select("sales", "state").filter($"state".equalTo("CA"))

    smallerDF.show()

    // Convert it to a Dataset by specifying the type of the rows -- use a case
    // class because we have one and it's most convenient to work with. Notice
    // you have to choose a case class that matches the remaining columns.
    // BUT also notice that the columns keep their order from the DataFrame --
    // later you'll see a Dataset[StateSales] of the same type where the
    // columns have the opposite order, because of the way it was created.
    val customerDS : Dataset[StateSales] = smallerDF.as[StateSales]

    println("*** Dataset schema")

    customerDS.printSchema()

    println("*** Dataset contents")

    customerDS.show()

    // Select and other operations can be performed directly on a Dataset too,
    // but be careful to read the documentation for Dataset -- there are
    // "typed transformations", which produce a Dataset, and
    // "untyped transformations", which produce a DataFrame. In particular,
    // you need to project using a TypedColumn to gate a Dataset.

    val verySmallDS : Dataset[Double] = customerDS.select($"sales".as[Double])

    println("*** Dataset after projecting one column")

    verySmallDS.show()

    // If you select multiple columns on a Dataset you end up with a Dataset
    // of tuple type, but the columns keep their names.
    val tupleDS : Dataset[(String, Double)] =
      customerDS.select($"state".as[String], $"sales".as[Double])

    println("*** Dataset after projecting two columns -- tuple version")

    tupleDS.show()

    // You can also cast back to a Dataset of a case class. Notice this time
    // the columns have the opposite order than the last Dataset[StateSales]
    val betterDS: Dataset[StateSales] = tupleDS.as[StateSales]

    println("*** Dataset after projecting two columns -- case class version")

    betterDS.show()

    // Converting back to a DataFrame without making other changes is really easy
    val backToDataFrame : DataFrame = tupleDS.toDF()

    println("*** This time as a DataFrame")

    backToDataFrame.show()

    // While converting back to a DataFrame you can rename the columns
    val renamedDataFrame : DataFrame = tupleDS.toDF("MyState", "MySales")

    println("*** Again as a DataFrame but with renamed columns")

    renamedDataFrame.show()

  }
}
