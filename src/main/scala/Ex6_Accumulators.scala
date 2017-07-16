import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{AccumulableParam, AccumulatorParam, SparkConf, SparkContext}

import scala.collection.mutable

import java.util.Collections

import scala.collection.JavaConversions._

//
// This example demonstrates accumulators that allow you to build global state
// safely and efficiently during parallel computations on RDDs.
//
// It begins with a simple "out of the box" accumulator and then covers two
// more complex ones that require you to extend the AccumulatorV2 class.
//
//
object Ex6_Accumulators {

  //
  // This approach to accumulating a set needs Java sets for synchronization.
  // It's based on a more abstract version int he Java sources that's not
  // exposed through the public API
  //
  class StringSetAccumulator extends AccumulatorV2[String, java.util.Set[String]] {
    private val _set = Collections.synchronizedSet(new java.util.HashSet[String]())
    override def isZero: Boolean = _set.isEmpty
    override def copy(): AccumulatorV2[String, java.util.Set[String]] = {
      val newAcc = new StringSetAccumulator()
      newAcc._set.addAll(_set)
      newAcc
    }
    override def reset(): Unit = _set.clear()
    override def add(v: String): Unit = _set.add(v)
    override def merge(other: AccumulatorV2[String, java.util.Set[String]]): Unit = {
      _set.addAll(other.value)
    }
    override def value: java.util.Set[String] = _set
  }

  //
  // Use a HashMap from Char to Int as an accumulator. The only tricky part is
  // that two hash maps have to be merged carefully to combine the counts from
  // both both sides
  //
  class CharCountingAccumulator extends AccumulatorV2[Char, java.util.Map[Char, Int]] {
    private val _map = Collections.synchronizedMap(new java.util.HashMap[Char, Int]())
    override def isZero: Boolean = _map.isEmpty
    override def copy(): AccumulatorV2[Char, java.util.Map[Char, Int]] = {
      val newAcc = new CharCountingAccumulator()
      newAcc._map.putAll(_map)
      newAcc
    }
    override def reset(): Unit = _map.clear()
    override def add(v: Char): Unit = {
      _map(v) = (if (_map.contains(v)) _map(v) else 0) + 1
    }
    override def merge(other: AccumulatorV2[Char, java.util.Map[Char, Int]]): Unit = {
      other.value.foreach {
        case (key, count) =>
          _map(key) = (if (_map.contains(key)) _map(key) else 0) + count
      }

    }
    override def value: java.util.Map[Char, Int] = _map
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Ex6_Accumulators").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // an RDD containing names
    val words = sc.parallelize(Seq("Fred", "Bob", "Francis",
      "James", "Frederick", "Frank", "Joseph"), 4)

    //
    // Example 1: use a simple counter to keep track of the number of words
    // starting with "F"
    //

    // an efficient counter -- this is one of the basic accumulators that
    // Spark provides "out of the box"
    val count = sc.longAccumulator

    println("*** Using a simple counter")
    words.filter(_.startsWith("F")).foreach(n => count.add(1))
    println("total count of names starting with F = " + count.value)

    //
    // Example 2: Accumulate the set of words starting with "F" -- you can
    // always count them later
    //

    // efficiently accumulate a set -- there's now "out of the box" way to
    // do this
    val names = new StringSetAccumulator
    sc.register(names)

    println("*** using a set accumulator")
    words.filter(_.startsWith("F")).foreach(names.add)
    println("All the names starting with 'F' are a set")
    names.value.iterator().foreach(println)

    //
    // Example 3: Accumulate a map from starting letter to cword count, and
    // extract the count for "F"
    //

    println("*** using a hash map accumulator")
    val counts = new CharCountingAccumulator
    sc.register(counts)
    words.foreach(w => counts.add(w.charAt(0)))
    counts.value.foreach {
      case (initial, times) => println(times + " names start with '" + initial + "'")
    }

    // TODO: spark.util.StatCounter
  }
}
