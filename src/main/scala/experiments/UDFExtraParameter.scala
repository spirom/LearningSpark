package experiments

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



object UDFExtraParameter {
  private case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("DataFrame-UDFExtraParameter")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    val pairs = Seq(
      (1, "One"),
      (2, "Two"),
      (3, "Three")
    )
    // make it an RDD and convert to a DataFrame
    val theMap = spark.sparkContext.parallelize(pairs, 4).collectAsMap()

    val data = Seq(1, 2, 3, 4, 5, 6, 99)

    val dataRDD = spark.sparkContext.parallelize(data, 4)

    val theUDF =
      udf { (datum: Int, map: Map[Int, String]) =>
        map.getOrElse(datum, "Other")}

    val dataDF = dataRDD.map((_,theMap)).toDF("data", "theMap");



    //val dataAndMap = dataWithMap.select(firstUDF(col("pairs")).as("data"),
    //  secondUDF(col("pairs")).as("theMap"))

    val result =
      dataDF.select(theUDF(col("data"), col("theMap")))

    result.foreach(println(_))
  }
}
