import org.apache.spark.{AccumulableParam, AccumulatorParam, SparkContext, SparkConf}

import scala.collection.mutable

object Ex6_Accumulators {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Ex6_Accumulators").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // an RDD with containing names
    val words = sc.parallelize(Seq("Fred", "Bob", "Francis",
      "James", "Frederick", "Frank", "Joseph"), 4)

    // efficiently accumulate a counter
    implicit def intAccum = new AccumulatorParam[Int] {
      def zero(initialValue: Int): Int = {
        0
      }

      def addInPlace(i1: Int, i2: Int): Int = {
        i1 + i2
      }
    }

    val count = sc.accumulator[Int](0)

    words.filter(_.startsWith("F")).foreach(n => count += 1)
    println("total count of names starting with F = " + count.value)

    // efficiently accumulate a set -- notice not just any Set will do
    val names =
      sc.accumulableCollection[mutable.HashSet[String],String](mutable.HashSet[String]())
    words.filter(_.startsWith("F")).foreach(names.add(_))
    println("All the names starting with 'F' are a set")
    names.value.foreach(println)

    // completely define accumulator behavior in a map where each entry
    // maps to a count; the corresponding counts need to be added when two
    // maps are merged
    implicit def mapAccum = new AccumulableParam[mutable.HashMap[Char, Int], Char] {
      def zero(t: mutable.HashMap[Char, Int])
      : mutable.HashMap[Char, Int] = {
        mutable.HashMap[Char, Int]()
      }

      def addInPlace(m1: mutable.HashMap[Char, Int],
                     m2: mutable.HashMap[Char, Int])
      : mutable.HashMap[Char, Int] = {
        m2.foreach {
          case (initial, times) =>
            m1(initial) = (if (m1.contains(initial)) m1(initial) else 0) + times
        }
        m1
      }

      def addAccumulator(m: mutable.HashMap[Char, Int], v: Char)
      : mutable.HashMap[Char, Int] = {
        m(v) = (if (m.contains(v)) m(v) else 0) + 1
        m
      }
    }

    val counts =
      sc.accumulable[mutable.HashMap[Char, Int], Char](mutable.HashMap[Char,Int]())
    words.foreach(w => counts.add(w.charAt(0)))
    counts.value.foreach {
      case (initial, times) => println(times + " names start with '" + initial + "'")
    }
  }
}
