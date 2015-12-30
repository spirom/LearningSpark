import org.apache.spark.{SparkContext, SparkConf}

object Ex4_MoreOperationsOnRDDs {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Ex4_MoreOperationsOnRDDs").setMaster("local[4]")
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

    // TODO: pay more attention to flatMap
    // flatMap: pay attention here -- the words don't always come
    // out in the right order, but the characters within a word do
    val words = sc.parallelize(Seq("Hello", "World"), 2)
    val chars = words.flatMap(w => w.iterator)
    println(chars.map(c => c.toString).reduce((s1, s2) => s1 + " " + s2))

    // groupBy
    val numbers = sc.parallelize(1 to 10, 4)
    val modThreeGroups = numbers.groupBy(_ % 3)
    // Note: pair RDDs are special in Spark
    modThreeGroups foreach {
      case (m, vals) => println("mod 3 = " + m + ", count = " + vals.count(_ => true))
    }

    // countByValue: how many of the mod results are three
    // notice use of Option type
    // TODO: what point was I trying to make?
    val mods = modThreeGroups.collect({
      case (m, vals) => vals.count(_ => true) }).countByValue
    println("results found 3 times: " + mods.get(3))
    println("results found 4 times: " + mods.get(4))
    println("results found 7 times: " + mods.get(7))

    // max, min
    println("maximum element = " + letters.max())

    // first
    println("first element = " + letters.first())

    // sample: notice this returns an RDD
    println("random [fractional] sample without replacement: ")
    val samp = letters.sample(false, 0.25, 42)
    samp.foreach(println)

    // sortBy
    // TODO: this deserves a clearer example of how spiffy it is
    println("first element when reversed = " + letters.sortBy(v => v, false).first())

    // take, takeSample: these just return arrays
    println("first five letters:")
    val first5 = letters.take(5)
    first5.foreach(println)

    println("random five letters without replacement:")
    val random5 = letters.takeSample(false, 5)
    random5.foreach(println)
  }
}
