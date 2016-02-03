package sql

import org.apache.spark.sql.catalyst.util.{GenericArrayData, ArrayData}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types._

import scala.beans.{BeanInfo, BeanProperty}

@SQLUserDefinedType(udt = classOf[MyPoint3DUDT])
private case class MyPoint3D(x: Double, y: Double, z: Double)

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

@BeanInfo
private case class MyLabeledPoint(
  @BeanProperty label: String,
  @BeanProperty point: MyPoint3D)

object UDT {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataFrame-UDT").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val points = Seq(
      new MyLabeledPoint("A", new MyPoint3D(1.0, 2.0, 3.0)),
      new MyLabeledPoint("B", new MyPoint3D(1.0, 0.0, 2.0))
    ).toDF()

    points.printSchema()

    points.show()

    points.registerTempTable("points")
    sqlContext.udf.register("myMagnitude", (p: MyPoint3D) =>
      math.sqrt(math.abs(p.x) * math.abs(p.y) + math.abs(p.z)))

    val labeledMagnitudes =
      sqlContext.sql("SELECT label, myMagnitude(point) as magnitude FROM points")

    labeledMagnitudes.printSchema()

    labeledMagnitudes.show()
  }

}
