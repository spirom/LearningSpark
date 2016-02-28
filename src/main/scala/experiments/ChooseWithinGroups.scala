package experiments

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

//
// Standard SQL problem of choosing a distinguished record within groups
//
object ChooseWithinGroups {
  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataFrame-GroupingAndAggregation").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val people = Seq(
      (5,"Bob","Jones","Canada",23),
      (7,"Fred","Smith","Canada",18),
      (5,"Robert","Andrews","USA",32)
    )
    val peopleRows = sc.parallelize(people, 4)

    val peopleDF = peopleRows.toDF("id", "first", "last", "country", "age")

    // this is the default, but just ion case ...
    sqlContext.setConf("spark.sql.retainGroupColumns", "true")

    // the renaming here shouldn't be necessary but if I
    // don't do it I seem to expose a Spark SQL bug
    val maxAge =
      peopleDF.select($"id" as "mid", $"age" as "mage")
        .groupBy("mid").agg(max($"mage") as "maxage")

    val maxAgeAll = maxAge.join(peopleDF,
      maxAge("maxage") === peopleDF("age") and maxAge("mid") === peopleDF("id"),
      "inner").select("id", "first", "last", "country", "age")

    maxAgeAll.show()

  }}
