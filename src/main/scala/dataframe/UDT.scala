package dataframe

import org.apache.spark.sql.catalyst.util.{GenericArrayData, ArrayData}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types._

//
// This example demonstrates how to define a basic user defined type (UDT) and
// how to use it in a query. The attributes of the underlying class are not
// directly accessible in the query, but you can access them by defining
// a user defined function (UDF) to be applied to instances of the UDT.
//
// NOTE: there is a more comprehensive example that involves layering of one
// UDT on top of the other in sql/UDT.scala.
//

//
// Underlying case class defining 3D points. The annotation conencts it with
// the UDT definition below.
//

/** *** SPECIAL NOTE ***
  * This feature has been removed in Spark 2.0.0 -- please see
  * https://issues.apache.org/jira/browse/SPARK-14155

@SQLUserDefinedType(udt = classOf[MyPoint3DUDT])
private case class MyPoint3D(x: Double, y: Double, z: Double)

//
// The UDT definition for 3D points: basically how to serialize and deserialize.
//

private class MyPoint3DUDT extends UserDefinedType[MyPoint3D] {
  override def sqlType: DataType = ArrayType(DoubleType, containsNull = false)

  override def serialize(obj: Any): ArrayData = {
    obj match {
      case features: MyPoint3D =>
        new GenericArrayData(Array(features.x, features.y, features.z))
    }
  }

  override def deserialize(datum: Any): MyPoint3D = {
    datum match {
      case data: ArrayData if data.numElements() == 3 => {
        val arr = data.toDoubleArray()
        new MyPoint3D(arr(0), arr(1), arr(2))
      }
    }
  }

  override def userClass: Class[MyPoint3D] = classOf[MyPoint3D]

  override def asNullable: MyPoint3DUDT = this
}

object UDT {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataFrame-UDT").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    //
    // First define some points, store them in a table and filter them
    // based on magnitude -- i.e.: distance form the origin
    //

    val p1 = new MyPoint3D(1.0, 2.0, 3.0)
    val p2 = new MyPoint3D(1.0, 0.0, 2.0)
    val p3 = new MyPoint3D(10.0, 20.0, 30.0)
    val p4 = new MyPoint3D(11.0, 22.0, 33.0)

    val points = Seq(
      ("P1", p1),
      ("P2", p2),
      ("P3", p3),
      ("P4", p4)
    ).toDF("label", "point")

    println("*** All the points as a dataframe")
    points.printSchema()
    points.show()

    // Define a UDF to get access to attributes of a point in a query
    val myMagnitude =
      udf { p: MyPoint3D =>
        math.sqrt(math.pow(p.x, 2) + math.pow(p.y, 2) + math.pow(p.z, 2))
      }

    val nearPoints =
      points.filter(myMagnitude($"point").lt(10))
            .select($"label", myMagnitude($"point").as("magnitude"))

    println("*** The points close to the origin, selected from the table")
    nearPoints.printSchema()
    nearPoints.show()
  }

}

**/