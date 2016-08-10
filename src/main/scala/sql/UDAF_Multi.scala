package sql

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

//
// This builds on UDAF.scala and UDAF2.scala to illustrate an
// aggregation function where the buffer has multiple values and the
// return type is complex. -- in this case two. It's not possible to return
// multiple columns but it _is_ possible to return a structure that can then be
// pulled apart by the SQL query into multiple columns.
//
object UDAF_Multi {

   //
   // A UDAF that returns the number of rows in each group, the number of
   // non-null values and the sum of the non-null values.
   //
   private class ScalaAggregateFunction extends UserDefinedAggregateFunction {

     // this aggregation function has just one parameter
     def inputSchema: StructType =
       new StructType().add("num", DoubleType)
     // the aggregation buffer in this case manages three partial sums
     def bufferSchema: StructType =
       new StructType()
         .add("rows", LongType)
         .add("count", LongType)
         .add("sum", DoubleType)
     // returns just a struct of three values
     def dataType: DataType =
       new StructType()
         .add("rows", LongType)
         .add("count", LongType)
         .add("sum", DoubleType)
     // always gets the same result
     def deterministic: Boolean = true

     // each partial sum is initialized to zero
     def initialize(buffer: MutableAggregationBuffer): Unit = {
       buffer.update(0, 0l)
       buffer.update(1, 0l)
       buffer.update(2, 0.0)
     }

     // an individual value is incorporated: always count it as a row, but
     // count and add the value only if it is not null
     def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
       buffer.update(0, buffer.getLong(0) + 1);
       if (!input.isNullAt(0)) {
         buffer.update(1, buffer.getLong(1) + 1);
         buffer.update(2, buffer.getDouble(2) + input.getDouble(0));
       }
     }

     // buffers are merged by adding the single values in them
     def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
       buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0));
       buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1));
       buffer1.update(2, buffer1.getDouble(2) + buffer2.getDouble(2));
     }

     // the entire aggregation buffer is returned as a struct
     def evaluate(buffer: Row): Any = {
       Row(
         buffer.getLong(0),
         buffer.getLong(1),
         buffer.getDouble(2)
       )
     }
   }

   def main (args: Array[String]) {
     val spark =
       SparkSession.builder()
         .appName("SQL-UDAF_Multi")
         .master("local[4]")
         .getOrCreate()

     import spark.implicits._

     // create an RDD of tuples with some data
     // NOTE the use of Some/None to create a nullable column
     val custs = Seq(
       (1, "Widget Co", Some(120000.00), 0.00, "AZ"),
       (2, "Acme Widgets", Some(800.00), 500.00, "CA"),
       (3, "Widgetry", Some(200.00), 200.00, "CA"),
       (4, "Widgets R Us", None, 0.0, "CA"),
       (5, "Ye Olde Widgete", Some(500.00), 0.0, "MA"),
       (6, "Charlestown Widget", None, 0.0, "MA")
     )
     val customerRows = spark.sparkContext.parallelize(custs, 4)
     val customerDF = customerRows.toDF("id", "name", "sales", "discount", "state")

     val mystats = new ScalaAggregateFunction()

     customerDF.printSchema()

     // register as a temporary table

     customerDF.createOrReplaceTempView("customers")

     spark.udf.register("stats", mystats)

     // now use it in a query
     val sqlResult =
       spark.sql(
         s"""
           | SELECT state, stats(sales) AS s
           | FROM customers
           | GROUP BY state
          """.stripMargin)
     sqlResult.printSchema()
     println()
     sqlResult.show()

     // getting separate columns
     // now use it in a query
     val sqlResult2 =
       spark.sql(
         s"""
            | SELECT state, s.rows, s.count, s.sum FROM (
            |   SELECT state, stats(sales) AS s
            |   FROM customers
            |   GROUP BY state
            | ) g
          """.stripMargin)
     sqlResult2.printSchema()
     println()
     sqlResult2.show()
   }

 }
