import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object Ex2_Computations {
  //
  // utilities for printing out a dependency tree
  //
  private def showDep[T](r: RDD[T], depth: Int) : Unit = {
    println("".padTo(depth, ' ') + "RDD id=" + r.id)
    r.dependencies.foreach(dep => {
      showDep(dep.rdd, depth + 1)
    })
  }
  def showDep[T](r: RDD[T]) : Unit = {
    showDep(r, 0)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Ex2_Computations").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // set up a simple computation
    val numbers = sc.parallelize(1 to 10, 4)
    val bigger = numbers.map(n => n * 100)
    val biggerStill = bigger.map(n => n + 1)

    println("Debug string for the RDD 'biggerStill'")
    println(biggerStill.toDebugString)

    val s = biggerStill.reduce(_ + _)

    println("sum = " + s)

    println("IDs of the various RDDs")
    println("numbers: id=" + numbers.id)
    println("bigger: id=" + bigger.id)
    println("biggerStill: id=" + biggerStill.id)
    println("dependencies working back from RDD 'biggerStill'")
    showDep(biggerStill)

    val moreNumbers = bigger ++ biggerStill
    println("The RDD 'moreNumbers' has mroe complex dependencies")
    println(moreNumbers.toDebugString)
    println("moreNumbers: id=" + moreNumbers.id)
    showDep(moreNumbers)

    moreNumbers.cache()
    // things in cache can be lost so dependency tree is not discarded
    println("cached it: the dependencies don't change")
    println(moreNumbers.toDebugString)
    showDep(moreNumbers)

    println("has RDD 'moreNumbers' been checkpointed? : " + moreNumbers.isCheckpointed)
    // set moreNumbers up to be checkpointed
    sc.setCheckpointDir("/tmp/sparkcps")
    moreNumbers.checkpoint()
    // it will only happen after we force the values to be computed
    println("NOW has it been checkpointed? : " + moreNumbers.isCheckpointed)
    moreNumbers.count()
    println("NOW has it been checkpointed? : " + moreNumbers.isCheckpointed)
    println(moreNumbers.toDebugString)
    showDep(moreNumbers)

    // again, calculations are not done until strictly necessary
    println("this shouldn't throw an exception")
    val thisWillBlowUp = numbers map {
      case (7) => { throw new Exception }
      case (n) => n
    }

    // notice it didn't blow up yet even though there's a 7
    println("the exception should get thrown now")
    try {
      println(thisWillBlowUp.count())
    } catch {
      case (e: Exception) => println("Yep, it blew up now")
    }

  }
}
