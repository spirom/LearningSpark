package dataset

import org.apache.spark.sql.SparkSession

//
// Create Datasets of primitive type and tuple type ands show simple operations.
//
object CaseClass {

  // define a case class for the elements of the Dataset
  // NOTE: this needs to be outside the scope of the method where the
  // Dataset is created
  case class Number(i: Int, english: String, french: String)

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("Dataset-CaseClass")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    val numbers = Seq(
      Number(1, "one", "un"),
      Number(2, "two", "deux"),
      Number(3, "three", "trois"))
    val numberDS = numbers.toDS()

    println("*** case class Dataset types")
    numberDS.dtypes.foreach(println(_))

    // Since we used a case class we can query using the field names
    // as column names
    println("*** filter by one column and fetch another")
    numberDS.where($"i" > 2).select($"english", $"french").show()

    println("*** could have used SparkSession.createDataset() instead")
    val anotherDS = spark.createDataset(numbers)

    println("*** case class Dataset types")
    anotherDS.dtypes.foreach(println(_))
  }
}
