package sql

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

//
// This builds on UDAF.scala to illustrate an aggregation function with
// multiple parameters -- in this case two. Notice there are three critical
// changes:
//    1) The definition of inputSchema adds a second field (but bufferSchema
//       remains unchanged as we are still just summing numbers)
//    2) The update() method now examines positions 0 and 1 of input
//    3) In the SQL, the function is now passed a second parameter
//
object UDAF2 {

   //
   // A UDAF that sums sales over $500
   //
   private class ScalaAggregateFunction extends UserDefinedAggregateFunction {

     // this aggregation function has two parameters
     def inputSchema: StructType =
       new StructType().add("sales", DoubleType).add("state", StringType)
     // the aggregation buffer can also have multiple values in general but
     // this one just has one: the partial sum
     def bufferSchema: StructType =
       new StructType().add("sumLargeSales", DoubleType)
     // returns just a double: the sum
     def dataType: DataType = DoubleType
     // always gets the same result
     def deterministic: Boolean = true

     // each partial sum is initialized to zero
     def initialize(buffer: MutableAggregationBuffer): Unit = {
       buffer.update(0, 0.0)
     }

     // these states will get special treatment
     val westernStates = Set("WA", "OR", "CA")

     // an individual sales value is incorporated by adding it if it exceeds
     // a threshold that now depends on the state
     def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
       val sum = buffer.getDouble(0)
       if (!input.isNullAt(0)) {
         val westernState =
           if (input.isNullAt(1)) {
             false
           } else {
             val state = input.getString(1)
             westernStates.contains(state)
           }
         val sales = input.getDouble(0)
         if ((westernState && sales > 1000.0) || (!westernState && sales > 400.0)) {
           buffer.update(0, sum+sales)
         }
       }
     }

     // buffers are merged by adding the single values in them
     def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
       buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0))
     }

     // the aggregation buffer just has one value: so return it
     def evaluate(buffer: Row): Any = {
       buffer.getDouble(0)
     }
   }

   def main (args: Array[String]) {
     val spark =
       SparkSession.builder()
         .appName("SQL-UDAF2")
         .master("local[4]")
         .getOrCreate()

     import spark.implicits._

     // create an RDD of tuples with some data
     val custs = Seq(
       (1, "Widget Co", 120000.00, 0.00, "AZ"),
       (2, "Acme Widgets", 800.00, 500.00, "CA"),
       (3, "Widgetry", 200.00, 200.00, "CA"),
       (4, "Widgets R Us", 410500.00, 0.0, "CA"),
       (5, "Ye Olde Widgete", 500.00, 0.0, "MA"),
       (6, "Charlestown Widget", 100.00, 0.0, "MA")
     )
     val customerRows = spark.sparkContext.parallelize(custs, 4)
     val customerDF = customerRows.toDF("id", "name", "sales", "discount", "state")

     val mysum = new ScalaAggregateFunction()

     customerDF.printSchema()

     // register as a temporary table

     customerDF.createOrReplaceTempView("customers")

     spark.udf.register("adjusted_sum", mysum)

     // now use it in a query
     val sqlResult =
       spark.sql(
         s"""
           | SELECT state, adjusted_sum(sales, state) AS bigsales
           | FROM customers
           | GROUP BY state
          """.stripMargin)
     sqlResult.printSchema()
     println()
     sqlResult.show()

   }

 }
