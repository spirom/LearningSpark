package dataframe

import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, SparkConf}

//
// Here we create a DataFrame from an RDD[Row] and a synthetic schema.
// This time we explore more interesting schema possibilities: for a basic
// example please see FromRowsAndSchema.scala .
//
object ComplexSchema {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataFrame-ComplexSchema").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //
    // Example 1: nested StructType for nested rows
    //
    val rows1 = Seq(
      Row(1, Row("a", "b"), 8.00, Row(1,2)),
      Row(2, Row("c", "d"), 9.00, Row(3,4))

    )
    val rows1Rdd = sc.parallelize(rows1, 4)

    val schema1 = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("s1", StructType(
          Seq(
            StructField("x", StringType, true),
            StructField("y", StringType, true)
          )
        ), true),
        StructField("d", DoubleType, true),
        StructField("s2", StructType(
          Seq(
            StructField("u", IntegerType, true),
            StructField("v", IntegerType, true)
          )
        ), true)
      )
    )

    val df1 = sqlContext.createDataFrame(rows1Rdd, schema1)

    println("Schema with nested struct")
    df1.printSchema()

    println("DataFrame with nested Row")
    df1.show()

    println("Select the column with nested Row at the top level")
    df1.select("s1").show()

    println("Select deep into the column with nested Row")
    df1.select("s1.x").show()

  }

}