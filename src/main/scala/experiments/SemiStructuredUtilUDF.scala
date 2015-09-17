package experiments

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

//
// Show various ways to query in SQL using user-defined functions UDFs.
//

object SemiStructuredUtilUDF {

  def isAtomic(o: AnyRef) : Boolean = {
    o match {
      case l:ArrayBuffer[_] => false
      case _ => true
    }
  }

  def isString(o: AnyRef) : Boolean = {
    o match {
      case s:String => true
      case _ => false
    }
  }

  //def isInt(o:AnyRef) : Boolean = {
  //  o match {
  //    case i:Int => true
  //    case _ => false
  //  }
  //}

  def isArray(o:AnyRef) : Boolean = {
    o match {
      case l:ArrayBuffer[_] => true
      case _ => false
    }
  }

  def arrayLength(o: AnyRef) : Int = {
    o match {
      case l:ArrayBuffer[_] => l.size
      case null => 0
      case _ => 1
    }
  }

  def isStruct(o: AnyRef) : Boolean = {
    o match {
      case r:Row => true
      case _ => false
    }
  }

  def arrayContains(a: AnyRef, v: AnyRef) : Boolean = {
    a match {
      case l:ArrayBuffer[_] => l.contains(v)
      case _ => false
    }
  }

  def struct(a:AnyRef) : Boolean = {
    println("hello")
    true
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("UDF").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val transactions = sqlContext.read.json("src/main/resources/data/mixed.json")
    transactions.printSchema()
    transactions.registerTempTable("transactions")


    sqlContext.udf.register("struct", struct _)



    val all =
      sqlContext.sql("SELECT a, id, struct(address) FROM transactions")
    all.foreach(println)

    sqlContext.udf.register("isAtomic", isAtomic _)
    sqlContext.udf.register("arrayLength", arrayLength _)

    val lotsOfOrders =
      sqlContext.sql("SELECT id FROM transactions WHERE arrayLength(orders) > 2")
    //lotsOfOrders.foreach(println)
  }

}
