import scala.collection.Iterator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkException, SparkContext, SparkConf}

import scala.collection.mutable.ListBuffer

object Ex3_CombiningRDDs {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Ex3_CombiningRDDs").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // put some data in an RDD
    val letters = sc.parallelize('a' to 'z', 8)

    // another RDD of the same type
    val vowels = sc.parallelize(Seq('a', 'e', 'i', 'o', 'u'), 4)

    // subtract one from another, getting yet another RDD of the same type
    val consonants = letters.subtract(vowels)
    println("There are " + consonants.count() + " consonants")

    val vowelsNotLetters = vowels.subtract(letters)
    println("There are " + vowelsNotLetters.count() + " vowels that aren't letters")

    // union
    val lettersAgain = consonants ++ vowels
    println("There really are " + lettersAgain.count() + " letters")

    // union with duplicates, removed
    val tooManyVowels = vowels ++ vowels
    println("There aren't really " + tooManyVowels.count() + " vowels")
    val justVowels = tooManyVowels.distinct()
    println("There are actually " + justVowels.count() + " vowels")

    // subtraction with duplicates
    val what = tooManyVowels.subtract(vowels)
    println("There are actually " + what.count() + " whats")

    // intersection
    val earlyLetters = sc.parallelize('a' to 'l', 2)
    val earlyVowels = earlyLetters.intersection(vowels)
    println("The early vowels:")
    earlyVowels.foreach(println)

    // RDD of a different type
    val numbers = sc.parallelize(1 to 2, 2)

    // cartesian product
    val cp = vowels.cartesian(numbers)
    println("Product has " + cp.count() + " elements")

    // index the letters
    val indexed = letters.zipWithIndex()
    println("indexed letters")
    indexed foreach {
      case (c, i) => println(i + ":  " + c)
    }

    // another RDD, same size and partitioning as letters
    val twentySix = sc.parallelize(101 to 126, 8)

    // zip the letters and numbers
    val differentlyIndexed = letters.zip(twentySix)
    differentlyIndexed foreach {
      case (c, i) => println(i + ":  " + c)
    }

    // we can't do this if the two RDDs don't have the same partitioning --
    // this is to remind us that it would be enormously costly in terms
    // of communication, so, as we'll see in later examples, we have to
    // fix the partitioning ourselves
    val twentySixBadPart = sc.parallelize(101 to 126, 3)
    val cantGet = letters.zip(twentySixBadPart)
    try {
      cantGet foreach {
        case (c, i) => println(i + ":  " + c)
      }
    } catch {
      case iae: IllegalArgumentException =>
        println("Exception caught: " + iae.getMessage)
    }

    // the zipped RDDs also need to have the same number of elements
    val unequalCount = earlyLetters.zip(numbers)
    try {
      unequalCount foreach {
        case (c, i) => println(i + ":  " + c)
      }
    }
    catch {
      case se: SparkException => {
        val t = se.getMessage
        println("Exception caught: " + se.getMessage)
      }
    }

    // zipPartitions gives us more control, se we can deal with weird cases
    // BUT the result may be surprising because each PARTITION also has
    // unequal numbers of elements, and the function 'zipFunc' gets
    // applied once per partition!
    // also notice the amount of type annotation to make the Scala compiler
    // happy -- it's an interesting exercise to remove some of them and read
    // the complaints

    def zipFunc(lIter: Iterator[Char], nIter: Iterator[Int]) :
      Iterator[(Char, Int)] = {
      val res = new ListBuffer[(Char, Int)]
      while (lIter.hasNext || nIter.hasNext) {
        if (lIter.hasNext && nIter.hasNext) {
          // easy case
          res += ((lIter.next(), nIter.next()))
        } else if (lIter.hasNext) {
          res += ((lIter.next(), 0))
        } else if (nIter.hasNext) {
          res += ((' ', nIter.next()))
        }
      }
      res.iterator
    }

    val unequalOK = earlyLetters.zipPartitions(numbers)(zipFunc)
    println("this may not be what you expected with unequal length RDDs")
    unequalOK foreach {
      case (c, i) => println(i + ":  " + c)
    }
  }
}