package special

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext, SparkConf}

// This gives is access to the PairRDDFunctions
import org.apache.spark.SparkContext._

import scala.collection.{mutable, Iterator}

// Partition a pair RDD with an integer key with the given
// partition count
class KeyPartitioner(np: Int) extends Partitioner {
  def numPartitions = np

  def getPartition(key: Any) : Int = {
    key match {
      case k:Int => k % numPartitions
      case _ => throw new ClassCastException
    }
  }
}

object PairRDD {
  def analyze[T](r: RDD[T]) : Unit = {
    val partitions = r.glom()
    println(partitions.count() + " partitions")

    // use zipWithIndex() to see the index of each partition
    // we need to loop sequentially so we can see them in order: use collect()
    partitions.zipWithIndex().collect().foreach {
      case (a, i) => {
        println("Partition " + i + " contents (count " + a.count(_ => true) + "):" +
          a.foldLeft("")((e, s) => e + " " + s))
      }
    }
  }

  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("Streaming").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val pairs = Seq((1,9), (1,2), (1,1), (2,3), (2,4), (3,1), (3,5), (6,2), (6,1), (6,4), (8,1))

    // a randomly partitioned pair RDD
    val pairsRDD = sc.parallelize(pairs, 4)
    // let's say we just want the pair with minimum value for each key
    // we can use one of the handy methods in PairRDDFunctions
    val reducedRDD = pairsRDD.reduceByKey(Math.min(_,_))

    // look at the partitioning of the two RDDs: it involved communication
    analyze(pairsRDD)
    analyze(reducedRDD)

    // if the original RDD was partitioned by key there's another approach:
    // it's not worth-while to repartition just for
    // this, be let's look at what we can do if it WAS partitioned by key
    val keyPartitionedPairs = pairsRDD.partitionBy(new KeyPartitioner(4))
    analyze(keyPartitionedPairs)

    // we can choose the min pair in each partition independently, so
    // no communication is required
    def minValFunc(i: Iterator[(Int, Int)]) : Iterator[(Int, Int)] = {
      val m = new mutable.HashMap[Int, Int]()
      i.foreach {
        case (k, v) => if (m.contains(k)) m(k) = Math.min(m(k), v) else m(k) = v
      }
      m.iterator
    }
    val reducedInPlace = keyPartitionedPairs.mapPartitions(minValFunc)

    // notice how the partitioning is retained
    analyze(reducedInPlace)

    // another "out of the box" approach to the reduction is to use
    // "aggregateByKey", which guarantees that all of the partitions
    // can be reduced separately AND IN PARALLEL, and then the partial
    // results can be combined -- essentially this relaxes
    // the strict condition imposed on "reduceByKey" that the supplied
    // function must be associative
    val reducedRDD2 = pairsRDD.aggregateByKey(Int.MaxValue)(Math.min(_,_), Math.min(_,_))
    analyze(reducedRDD2)

    // TODO: come up with an interesting example of aggregateByKey that
    // TODO: actually takes advantage of its generality

    // TODO: go to town with PairRDD and PairRDDFunctions
  }
}
