import org.apache.spark.{SparkContext, SparkConf}

object Ex3_MoreOperationsOnRDDs {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Ex2_MoreOperationsOnRDDs").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // put some data in an RDD
    val letters = sc.parallelize('a' to 'z', 8)

    // vowels, but NOT an RDD
    // this is important because you can't access RDDs in
    // operations on individual elements of an RDD
    val vowels = Seq('a', 'e', 'i', 'o', 'u')

    val consonants = letters.filter(c => !vowels.contains(c))
    println("There are " + consonants.count() + " consonants")

    // can use a partial function to filter and transform
    // notice the new RDD in this case has a different type
    val consonantsAsDigits = letters collect {
      case c:Char if !vowels.contains(c) => c.asDigit
    }
    consonantsAsDigits.foreach(println)

  }
}
