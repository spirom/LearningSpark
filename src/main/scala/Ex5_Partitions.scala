import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.SparkContext._

import scala.collection.{mutable, Iterator}

object Ex5_Partitions {

  // create an easy way to look at the partitioning of an RDD
  def analyze[T](r: RDD[T]) : Unit = {
    val partitions = r.glom()
    println(partitions.count() + " partitions")

    // use zipWithIndex() to see the index of each partition
    // we need to loop sequentially so we can see them in order: use collect()
    partitions.zipWithIndex().collect().foreach {
      case (a, i) => {
        println("Partition " + i + " contents:" +
          a.foldLeft("")((e, s) => e + " " + s))
      }
    }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Ex5_Partitions").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // look at the distribution of numbers across partitions
    val numbers =  sc.parallelize(1 to 100, 4)
    println("original RDD:")
    analyze(numbers)

    val some = numbers.filter(_ < 34)
    println("filtered RDD")
    analyze(some)

    // subtract doesn't do what you might hope
    val diff = numbers.subtract(some)
    println("the complement:")
    analyze(diff)
    println("it is a " + diff.getClass.getCanonicalName)

    // setting the number of partitions doesn't help (it was right anyway)
    val diffSamePart = numbers.subtract(some, 4)
    println("the complement (explicit but same number of partitions):")
    analyze(diffSamePart)

    // we can change the number but it also doesn't help
    // other methods such as intersection and groupBy allow this
    val diffMorePart = numbers.subtract(some, 6)
    println("the complement (different number of partitions):")
    analyze(diffMorePart)
    println("it is a " + diffMorePart.getClass.getCanonicalName)

    // but there IS a way to calculate the difference without
    // introducing communications
    def subtractFunc(wholeIter: Iterator[Int], partIter: Iterator[Int]) :
    Iterator[Int] = {
      val partSet = new mutable.HashSet[Int]()
      partSet ++= partIter
      wholeIter.filterNot(partSet.contains(_))
    }

    val diffOriginalPart = numbers.zipPartitions(some)(subtractFunc)
    println("complement with original partitioning")
    analyze(diffOriginalPart)
    println("it is a " + diffOriginalPart.getClass.getCanonicalName)

    // TODO: coalesce

    // repartition
    val threePart = numbers.repartition(3)
    println("numbers in three partitions")
    analyze(threePart)
    println("it is a " + threePart.getClass.getCanonicalName)

    val twoPart = some.coalesce(2, true)
    println("subset in two partitions after a shuffle")
    analyze(twoPart)
    println("it is a " + twoPart.getClass.getCanonicalName)

    val twoPartNoShuffle = some.coalesce(2, false)
    println("subset in two partitions without a shuffle")
    analyze(twoPartNoShuffle)
    println("it is a " + twoPartNoShuffle.getClass.getCanonicalName)

    // a ShuffledRDD with interesting characteristics
    val groupedNumbers = numbers.groupBy(n => if (n % 2 == 0) "even" else "odd")
    println("numbers grouped into 'odd' and 'even'")
    analyze(groupedNumbers)
    println("it is a " + groupedNumbers.getClass.getCanonicalName)

    // preferredLocations
    numbers.partitions.foreach(p => {
      println("Partition: " + p.index)
      numbers.preferredLocations(p).foreach(s => println("  Location: " + s))
    })

    // mapPartitions to achieve in-place grouping
    // TODO: fix this example ot make it a bit more natural

    val pairs = sc.parallelize(for (x <- 1 to 6; y <- 1 to x) yield ("S" + x, y), 4)
    analyze(pairs)

    val rollup = pairs.foldByKey(0, 4)(_ + _)
    println("just rolling it up")
    analyze(rollup)

    def rollupFunc(i: Iterator[(String, Int)]) : Iterator[(String, Int)] = {
      val m = new mutable.HashMap[String, Int]()
      i.foreach {
        case (k, v) => if (m.contains(k)) m(k) = m(k) + v else m(k) = v
      }
      m.iterator
    }

    val inPlaceRollup = pairs.mapPartitions(rollupFunc, true)
    println("rolling it up really carefully")
    analyze(inPlaceRollup)

  }
}
