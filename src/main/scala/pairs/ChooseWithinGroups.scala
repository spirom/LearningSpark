package pairs

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//
// Various approaches to choosing a distinguished record within groups,
// based on various approaches from PairRDDFunctions
//
object ChooseWithinGroups {
  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Pairs-ChooseWithinGroups").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val people = Seq(
      (5,"Bob","Jones","Canada",23),
      (7,"Fred","Smith","Canada",18),
      (5,"Robert","Andrews","USA",32)
    )
    val peopleRows = sc.parallelize(people, 4)

    type Payload = (String, String, String, Int)

    //
    // For all the approaches we need to get the data into an RDD[Pair[U,V]].
    // Since the problem requires the groups to be defined byt he first element,
    // that needs to be the first of the pair. Everything else ends up in the
    // second.
    //

    val pairs: RDD[(Int, Payload)] = peopleRows.map({
      case (id: Int, first: String, last: String, country: String, age: Int) =>
        (id, (first, last, country, age))
    }
    )

    // reduceByKey solution

    {

      def combine(p1: Payload, p2: Payload): Payload = {
        if (p1._4 > p2._4) p1 else p2
      }

      val withMax: RDD[(Int, Payload)] =
        pairs.reduceByKey(combine)

      withMax.collect().foreach(println)
    }

    // aggregateByKey solution

    {
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
    }

    // combineByKey solution

    // foldByKey solution

  }}
