package sql

import org.apache.spark.sql.catalyst.InternalRow
//import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.util.{GenericArrayData, ArrayData}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types._

//
// This example demonstrates how to define a basic user defined type (UDT) and
// how to use it in a query. The attributes of the underlying class are not
// directly accessible in the query, but you can access them by defining
// a user defined function (UDF) to be applied to instances of the UDT.
//
// Then a second UDT is layered on top of the first one, and stored and
// queried in the same way.
//
// NOTE about representation: the UDTs need to choose a underlying
// representation from the Spark SQL type system. Two different approaches
// are used int he two UDTs here just for illustration: arrays and structs.
// There's nothing deeply significant about these choices -- either approach
// could just as well have been used for both UDTs.
//

//
// Underlying case class defining 3D points. The annotation connects it with
// the UDT definition below.
//

/** *** SPECIAL NOTE ***
  * This feature has been removed in Spark 2.0.0 -- please see
  * https://issues.apache.org/jira/browse/SPARK-14155

@SQLUserDefinedType(udt = classOf[MyPoint3DUDT])
private case class MyPoint3D(x: Double, y: Double, z: Double) {
  def magnitude = math.sqrt(
    math.pow(x, 2) + math.pow(y, 2) + math.pow(z, 2))
  def distance(o: MyPoint3D) = math.sqrt(
    math.pow(o.x - x, 2) + math.pow(o.y - y, 2) + math.pow(o.z - z, 2))
}

//
// The UDT definition for 3D points: basically how to serialize and deserialize.
//

private class MyPoint3DUDT extends UserDefinedType[MyPoint3D] {
  //
  // Choose an array as the underlying representation
  //
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

// this is used below to pick up the schema of a point
private object MyPoint3DUDT extends MyPoint3DUDT

//
// Underlying case class defining 3D lien segments int yerms of 3D points.
//

@SQLUserDefinedType(udt = classOf[MyLine3DUDT])
private case class MyLine3D(p1: MyPoint3D, p2:MyPoint3D) {
  def length = p1.distance(p2)
}

//
// The UDT definition fro 3D lien segments. Notice how serialization and
// deserialization depend on those for points.
//

private class MyLine3DUDT extends UserDefinedType[MyLine3D] {
  //
  // Use a struct (row) as the underlying representation
  //
  override def sqlType: DataType =
    StructType(Seq(
      StructField("p1", MyPoint3DUDT.sqlType, nullable = false),
      StructField("p2", MyPoint3DUDT.sqlType, nullable = false)
    ))

  override def serialize(obj: Any): GenericMutableRow = {
    obj match {
      case MyLine3D(p1, p2) =>
        new GenericMutableRow(
          Array[Any](
            MyPoint3DUDT.serialize(p1),
            MyPoint3DUDT.serialize(p2)
          )
        )
    }
  }

  override def deserialize(datum: Any): MyLine3D = {
    datum match {
      case row: InternalRow if row.numFields == 2 =>
      {
        val schema = sqlType.asInstanceOf[StructType]
        val pointSeq = row.toSeq(schema)
        val p1 = MyPoint3DUDT.deserialize(pointSeq(0))
        val p2 = MyPoint3DUDT.deserialize(pointSeq(1))
        val line = new MyLine3D(p1, p2)
        line
      }
    }
  }

  override def userClass: Class[MyLine3D] = classOf[MyLine3D]

  override def asNullable: MyLine3DUDT = this
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

    points.createOrReplaceTempView("points")

    // Define a UDF to get access to attributes of a point in a query
    sqlContext.udf.register("myMagnitude", (p: MyPoint3D) => p.magnitude)

    val nearPoints =
      sqlContext.sql(
        """
          | SELECT label, myMagnitude(point) as magnitude
          | FROM points
          | WHERE myMagnitude(point) < 10
        """.stripMargin)

    println("*** The points close to the origin, selected from the table")
    nearPoints.printSchema()
    nearPoints.show()

    //
    // This time define some lines, store them in a table and filter them
    // based on length
    //

    val lines = Seq(
      ("A", new MyLine3D(p1, p2)),
      ("B", new MyLine3D(p3, p4)),
      ("C", new MyLine3D(p1, p4)),
      ("D", new MyLine3D(p2, p3))
    ).toDF("label", "line")

    println("*** All the lines as a dataframe")
    lines.printSchema()
    lines.show()

    lines.createOrReplaceTempView("lines")

    // Define a UDF to get access to attributes of a point in a query
    sqlContext.udf.register("myLength", (l: MyLine3D) => l.length)

    val shortLines =
      sqlContext.sql(
        """
          | SELECT label, myLength(line) as length
          | FROM lines
          | WHERE myLength(line) < 5
        """.stripMargin)

    println("*** The short lines, selected from the table")
    shortLines.printSchema()
    shortLines.show()

  }

}

*/