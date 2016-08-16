package dataframe

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

//
// Here we create a DataFrame from an RDD[Row] and a synthetic schema.
// This time we explore more interesting schema possibilities: for a basic
// example please see FromRowsAndSchema.scala .
//
object ComplexSchema {
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("DataFrame-ComplexSchema")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    //
    // Example 1: nested StructType for nested rows
    //

    val rows1 = Seq(
      Row(1, Row("a", "b"), 8.00, Row(1,2)),
      Row(2, Row("c", "d"), 9.00, Row(3,4))

    )
    val rows1Rdd = spark.sparkContext.parallelize(rows1, 4)

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

    println("Position of subfield 'd' is " + schema1.fieldIndex("d"))

    val df1 = spark.createDataFrame(rows1Rdd, schema1)

    println("Schema with nested struct")
    df1.printSchema()

    println("DataFrame with nested Row")
    df1.show()

    println("Select the column with nested Row at the top level")
    df1.select("s1").show()

    println("Select deep into the column with nested Row")
    df1.select("s1.x").show()

    println("The column function getField() seems to be the 'right' way")
    df1.select($"s1".getField("x")).show()

    //
    // Example 2: ArrayType
    //

    val rows2 = Seq(
      Row(1, Row("a", "b"), 8.00, Array(1,2)),
      Row(2, Row("c", "d"), 9.00, Array(3,4,5))

    )
    val rows2Rdd = spark.sparkContext.parallelize(rows2, 4)

    //
    // This time, instead of just using the StructType constructor, see
    // that you can use two different overloads of the add() method to add
    // the fields 'd' and 'a'
    //
    val schema2 = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("s1", StructType(
          Seq(
            StructField("x", StringType, true),
            StructField("y", StringType, true)
          )
        ), true)
      )
    )
      .add(StructField("d", DoubleType, true))
      .add("a", ArrayType(IntegerType))

    val df2 = spark.createDataFrame(rows2Rdd, schema2)

    println("Schema with array")
    df2.printSchema()

    println("DataFrame with array")
    df2.show()

    println("Count elements of each array in the column")
    df2.select($"id", size($"a").as("count")).show()

    println("Explode the array elements out into additional rows")
    df2.select($"id", explode($"a").as("element")).show()

    println("Apply a membership test to each array in a column")
    df2.select($"id", array_contains($"a", 2).as("has2")).show()

    // interestingly, indexing using the [] syntax seems not to be supported
    // (surprising only because that syntax _does_ work in Spark SQL)
    //df2.select("id", "a[2]").show()

    println("Use column function getItem() to index into array when selecting")
    df2.select($"id", $"a".getItem(2)).show()

    //
    // Example 3: MapType
    //

    val rows3 = Seq(
      Row(1, 8.00, Map("u" -> 1,"v" -> 2)),
      Row(2, 9.00, Map("x" -> 3, "y" -> 4, "z" -> 5))
    )
    val rows3Rdd = spark.sparkContext.parallelize(rows3, 4)

    val schema3 = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("d", DoubleType, true),
        StructField("m", MapType(StringType, IntegerType))
      )
    )

    val df3 = spark.createDataFrame(rows3Rdd, schema3)

    println("Schema with map")
    df3.printSchema()

    println("DataFrame with map")
    df3.show()

    println("Count elements of each map in the column")
    df3.select($"id", size($"m").as("count")).show()

    // notice you get one column from the keys and one from the values
    println("Explode the map elements out into additional rows")
    df3.select($"id", explode($"m")).show()

    // MapType is actually a more flexible version of StructType, since you
    // can select down into fields within a column, and the rows where
    // an element is missing just return a null
    println("Select deep into the column with a Map")
    df3.select($"id", $"m.u").show()

    println("The column function getItem() seems to be the 'right' way")
    df3.select($"id", $"m".getItem("u")).show()

  }

}