package sql

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

//
// NOTE: the type conversions here are a lot less lenient in Spark 1.5.0 than before
//
object Types {
  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("SQL-Types")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    val numericRows = Seq(
      Row(1.toByte, 2.toShort, 3, 4.toLong,
        BigDecimal(1), BigDecimal(2), 3.0f, 4.0)
    )
    val numericRowsRDD = spark.sparkContext.parallelize(numericRows, 4)

    val numericSchema = StructType(
      Seq(
        StructField("a", ByteType, true),
        StructField("b", ShortType, true),
        StructField("c", IntegerType, true),
        StructField("d", LongType, true),
        StructField("e", DecimalType(10, 5), true),
        StructField("f", DecimalType(20, 10), true),
        StructField("g", FloatType, true),
        StructField("h", DoubleType, true)
      )
    )

    val numericDF = spark.createDataFrame(numericRowsRDD, numericSchema)

    numericDF.printSchema()

    numericDF.show()

    numericDF.createOrReplaceTempView("numeric")

    spark.sql("SELECT * from numeric").show()

    val miscSchema = StructType(
      Seq(
        StructField("a", BooleanType, true),
        StructField("b", NullType, true),
        StructField("c", StringType, true),
        StructField("d", BinaryType, true)
      )
    )

    val complexScehma = StructType(
        Seq(
          StructField("a", StructType(
            Seq(
              StructField("u", StringType, true),
              StructField("v", StringType, true),
              StructField("w", StringType, true)
            )
          ), true),
          StructField("b", NullType, true),
          StructField("c", StringType, true),
          StructField("d", BinaryType, true)
        )
      )

  }
}
