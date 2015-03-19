package special

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable

// This gives is access to the PairRDDFunctions
import org.apache.spark.SparkContext._

// encapsulate a small sequence of pairs to be joined with pair RDDs --
// making this serializable effectively allows the hash table to be
// broadcast to each worker
// Reference: http://en.wikipedia.org/wiki/Hash_join
// (this is specifically an inner equi-join on pairs)
class HashJoiner[K,V](small: Seq[(K,V)]) extends java.io.Serializable {

  // stash it as a hash table, remembering that the keys may not be unique,
  // so we need to collect values for each key in a list
  val m = new mutable.HashMap[K, mutable.ListBuffer[V]]()
  small.foreach {
    case (k, v) => if (m.contains(k)) m(k) += v else m(k) = mutable.ListBuffer(v)
  }

  // when joining the RDD, remember that each key in it may or may not have
  // a matching key in the array, and we need a result tuple for each value
  // in the list contained in the corresponding hash table entry
  def joinOnLeft[U](large: RDD[(K,U)]) : RDD[(K, (U,V))] = {
    large.flatMap {
      case (k, u) =>
        m.get(k).flatMap(ll => Some(ll.map(v => (k, (u, v))))).getOrElse(mutable.ListBuffer())
    }
  }
}

object HashJoin {
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("HashJoin").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val smallRDD = sc.parallelize(
      Seq((1, 'a'), (1, 'c'), (2, 'a'), (3,'x'), (3,'y'), (4,'a')),
      4)

    val largeRDD = sc.parallelize(
      for (x <- 1 to 10000) yield (x % 4, x),
      4
    )

    // simply joining the two RDDs will be slow as it requires
    // lots of communication
    val joined = largeRDD.join(smallRDD)
    joined.collect().foreach(println)

    // If the smaller RDD is small enough we're better of with it not
    // being an RDD -- and we can implement a hash join by hand, effectively
    // broadcasting the hash table to each worker
    println("hash join result")
    // NOTE: it may be tempting to use "collectAsMap" below instead of "collect",
    // and simplify the joiner accordingly, but that only works if the keys
    // are unique
    val joiner = new HashJoiner(smallRDD.collect())
    val hashJoined = joiner.joinOnLeft(largeRDD)
    hashJoined.collect().foreach(println)


  }
}