package experiments

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

//
// Standard SQL problem of choosing a distinguished record within groups
//
object ChooseWithinGroups {
  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("Experiments")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    val people = Seq(
      (5,"Bob","Jones","Canada",23),
      (7,"Fred","Smith","Canada",18),
      (5,"Robert","Andrews","USA",32)
    )
    val peopleRows = spark.sparkContext.parallelize(people, 4)

    val peopleDF = peopleRows.toDF("id", "first", "last", "country", "age")

    // this is the default, but just ion case ...
    spark.conf.set("spark.sql.retainGroupColumns", "true")

    // the renaming here shouldn't be necessary but if I
    // don't do it I seem to expose a Spark SQL bug
    val maxAge =
      peopleDF.select($"id" as "mid", $"age" as "mage")
        .groupBy("mid").agg(max($"mage") as "maxage")

    val maxAgeAll = maxAge.join(peopleDF,
      maxAge("maxage") === peopleDF("age") and maxAge("mid") === peopleDF("id"),
      "inner").select("id", "first", "last", "country", "age")

    maxAgeAll.show()

    //
    // And now for a core Spark solution
    //

    type Payload = (String, String, String, Int)

    val pairs: RDD[(Int, Payload)] = peopleRows.map({
      case (id: Int, first: String, last: String, country: String, age: Int) =>
        (id, (first, last, country, age))
    }
    )

    def add(acc: Option[Payload], rec: Payload): Option[Payload] = {
      acc match {
        case None => Some(rec)
        case Some(previous) => if (rec._4 > previous._4) Some(rec) else acc
      }
    }

    def combine(acc1: Option[Payload], acc2: Option[Payload]): Option[Payload] = {
      (acc1, acc2) match {
        case (None, None) => None
        case (None, _) => acc2
        case (_, None) => acc1
        case (Some(p1), Some(p2)) => if (p1._4 > p2._4) acc1 else acc2
      }
    }

    val start: Option[Payload] = None

    val withMax: RDD[(Int, Option[Payload])] =
      pairs.aggregateByKey(start)(add, combine)

    withMax.collect().foreach(println)

  }}
