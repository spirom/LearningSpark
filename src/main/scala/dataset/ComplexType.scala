package dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

//
// Create Datasets of complex type using nested case classes, arrays, maps and
// Option, and query them. This example shows a lot of the power of the Dataset
// concept, since the expression of these complex types in terms of case
// classes seems so natural.
//
object ComplexType {

  // for all the examples
  case class Point(x: Double, y: Double)
  // for example 1
  case class Segment(from: Point, to: Point)
  // for example 2
  case class Line(name: String, points: Array[Point])
  // for example 3
  case class NamedPoints(name: String, points: Map[String, Point])
  // for example 4
  case class NameAndMaybePoint(name: String, point: Option[Point])

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("Dataset-ComplexType")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    //
    // Example 1: nested case classes
    //

    println("*** Example 1: nested case classes")

    val segments = Seq(
      Segment(Point(1.0, 2.0), Point(3.0, 4.0)),
      Segment(Point(8.0, 2.0), Point(3.0, 14.0)),
      Segment(Point(11.0, 2.0), Point(3.0, 24.0)))
    val segmentsDS = segments.toDS()

    segmentsDS.printSchema();

    // You can query using the field names of the case class as
    // as column names, ane we you descend down nested case classes
    // using getField() --
    // so the column from.x is specified using $"from".getField("x")
    println("*** filter by one column and fetch another")
    segmentsDS.where($"from".getField("x") > 7.0).select($"to").show()

    //
    // Example 2: arrays
    //

    println("*** Example 2: arrays")

    val lines = Seq(
      Line("a", Array(Point(0.0, 0.0), Point(2.0, 4.0))),
      Line("b", Array(Point(-1.0, 0.0))),
      Line("c", Array(Point(0.0, 0.0), Point(2.0, 6.0), Point(10.0, 100.0)))
    )
    val linesDS = lines.toDS()

    linesDS.printSchema()

    // notice here you can filter by the second element of the array, which
    // doesn't even exist in one of the rows
    println("*** filter by an array element")
    linesDS
      .where($"points".getItem(2).getField("y") > 7.0)
      .select($"name", size($"points").as("count")).show()

    //
    // Example 3: maps
    //

    println("*** Example 3: maps")

    val namedPoints = Seq(
      NamedPoints("a", Map("p1" -> Point(0.0, 0.0))),
      NamedPoints("b", Map("p1" -> Point(0.0, 0.0),
        "p2" -> Point(2.0, 6.0), "p3" -> Point(10.0, 100.0)))
    )
    val namedPointsDS = namedPoints.toDS()

    namedPointsDS.printSchema()

    println("*** filter and select using map lookup")
    namedPointsDS
      .where(size($"points") > 1)
      .select($"name", size($"points").as("count"), $"points".getItem("p1")).show()

    //
    // Example 4: Option
    //

    println("*** Example 4: Option")

    val maybePoints = Seq(
      NameAndMaybePoint("p1", None),
      NameAndMaybePoint("p2", Some(Point(-3.1, 99.99))),
      NameAndMaybePoint("p3", Some(Point(1.0, 2.0))),
      NameAndMaybePoint("p4", None)
    )
    val maybePointsDS = maybePoints.toDS()

    maybePointsDS.printSchema()

    println("*** filter by nullable column resulting from Option type")
    maybePointsDS
      .where($"point".getField("y") > 50.0)
      .select($"name", $"point").show()

    println("*** again its fine also to select through a column that's sometimes null")
    maybePointsDS.select($"point".getField("x")).show()

  }
}
