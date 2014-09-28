import org.apache.spark.{SparkContext, SparkConf}

object Ex2_CombiningRDDs {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Ex2_CombiningRDDs").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // put some data in an RDD
    val letters = sc.parallelize('a' to 'z', 8)

    // another RDD of the same type
    val vowels = sc.parallelize(Seq('a', 'e', 'i', 'o', 'u'), 4)

    // subtract one from another, getting yet another RDD of the same type
    val consonants = letters.subtract(vowels)
    println("There are " + consonants.count() + " consonants")

    // union
    val lettersAgain = consonants ++ vowels
    println("There really are " + lettersAgain.count() + " letters")

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
  }
}