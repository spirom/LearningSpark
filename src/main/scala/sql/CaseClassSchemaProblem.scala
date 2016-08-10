package sql

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

//
// ***NOTE***: this fails in Spark 1.3.0 with scala.MatchError
// It was written as an attempt to answer the following question:
// http://stackoverflow.com/questions/29310405/what-is-the-right-way-to-represent-an-any-type-in-spark-sql
//
// Filed as https://issues.apache.org/jira/browse/SPARK-6587 -- while I don't
// necessarily agree with the answer I have to concede that it's reasonable.
//
// Arguably, the error message has improved a bit in Spark 1.5.0.
//
// TODO: it may be interesting to see where DataSet takes this ...
//
object CaseClassSchemaProblem {

  private abstract class MyHolder

  private case class StringHolder(s: String) extends MyHolder

  private case class IntHolder(i: Int) extends MyHolder

  private case class BooleanHolder(b: Boolean) extends MyHolder

  private case class Thing(key: Integer, foo: MyHolder)

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("SQL-CaseClassSchemaProblem")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    val things = Seq(
      Thing(1, IntHolder(42)),
      Thing(2, StringHolder("hello")),
      Thing(3, BooleanHolder(false))
    )
    val thingsDF = spark.sparkContext.parallelize(things, 4).toDF()

    thingsDF.createOrReplaceTempView("things")

    val all = spark.sql("SELECT * from things")

    all.printSchema()

    all.show()
  }
}



