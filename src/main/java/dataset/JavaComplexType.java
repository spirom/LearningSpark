package dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.size;


//
// Examples of querying against more complex schema inferred from Java beans.
// Includes JavaBean nesting, arrays and maps.
//
public class JavaComplexType {

    //
    // A JavaBean for all the examples
    //
    public static class Point implements Serializable {
        private double x;
        private double y;

        public Point(double x, double y) {
            this.x = x;
            this.y = y;
        }

        public double getX() {
            return x;
        }

        public void setX(double x) {
            this.x = x;
        }

        public double getY() {
            return y;
        }

        public void setY(double y) {
            this.y = y;
        }
    }

    //
    // A JavaBean for Example 1
    //
    public static class Segment implements Serializable {
        private Point from;
        private Point to;

        public Segment(Point from,  Point to) {
            this.to = to;
            this.from = from;
        }

        public Point getFrom() {
            return from;
        }

        public void setFrom(Point from) {
            this.from = from;
        }

        public Point getTo() {
            return to;
        }

        public void setTo(Point to) {
            this.to = to;
        }
    }

    //
    // A JavaBean for Example 2
    //
    public static class Line implements Serializable {
        private String name;
        private Point[] points;

        public Line(String name, Point[] points) {
            this.name = name;
            this.points = points;
        }

        public String getName() { return name; }

        public void setName(String name) { this.name = name; }

        public Point[] getPoints() { return points; }

        public void setPoints(Point[] points) { this.points = points; }
    }

    //
    // A JavaBean for Example 3
    //
    public static class NamedPoints implements Serializable {
        private String name;
        private Map<String, Point> points;

        public NamedPoints(String name, Map<String, Point> points) {
            this.name = name;
            this.points = points;
        }

        public String getName() { return name; }

        public void setName(String name) { this.name = name; }

        public Map<String, Point> getPoints() { return points; }

        public void setPoints(Map<String, Point> points) { this.points = points; }
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
            .builder()
            .appName("Dataset-Java-ComplexType")
            .master("local[4]")
            .getOrCreate();

        //
        // Example 1: nested Java beans
        //

        System.out.println("*** Example 1: nested Java beans");

        Encoder<Segment> segmentEncoder = Encoders.bean(Segment.class);

        List<Segment> data = Arrays.asList(
            new Segment(new Point(1.0, 2.0), new Point(3.0, 4.0)),
            new Segment(new Point(8.0, 2.0), new Point(3.0, 14.0)),
            new Segment(new Point(11.0, 2.0), new Point(3.0, 24.0)));

        Dataset<Segment> ds = spark.createDataset(data, segmentEncoder);

        System.out.println("*** here is the schema inferred from the bean");
        ds.printSchema();

        System.out.println("*** here is the data");
        ds.show();

        // Use the convenient bean-inferred column names to query
        System.out.println("*** filter by one column and fetch others");
        ds.where(col("from").getField("x").gt(7.0)).select(col("to")).show();

        //
        // Example 2: arrays
        //

        System.out.println("*** Example 2: arrays");

        Encoder<Line> lineEncoder = Encoders.bean(Line.class);
        List<Line> lines = Arrays.asList(
            new Line("a", new Point[]{new Point(0.0, 0.0), new Point(2.0, 4.0)}),
            new Line("b", new Point[]{new Point(-1.0, 0.0)}),
            new Line("c", new Point[]
                    {new Point(0.0, 0.0), new Point(2.0, 6.0), new Point(10.0, 100.0)})
        );

        Dataset<Line> linesDS = spark.createDataset(lines, lineEncoder);

        System.out.println("*** here is the schema inferred from the bean");
        linesDS.printSchema();

        System.out.println("*** here is the data");
        linesDS.show();

        // notice here you can filter by the second element of the array, which
        // doesn't even exist in one of the rows
        System.out.println("*** filter by an array element");
        linesDS
            .where(col("points").getItem(2).getField("y").gt(7.0))
            .select(col("name"), size(col("points")).as("count")).show();

        //
        // Example 3: maps
        //

        if (false) {

            //
            // In Spark 2.0 this throws
            // java.lang.UnsupportedOperationException: map type is not supported currently
            // See https://issues.apache.org/jira/browse/SPARK-16706 -- and
            // notice it has been marked Fixed for Spark 2.1.0.
            //

            System.out.println("*** Example 3: maps");

            Encoder<NamedPoints> namedPointsEncoder = Encoders.bean(NamedPoints.class);
            HashMap<String, Point> points1 = new HashMap<>();
            points1.put("p1", new Point(0.0, 0.0));
            HashMap<String, Point> points2 = new HashMap<>();
            points2.put("p1", new Point(0.0, 0.0));
            points2.put("p2", new Point(2.0, 6.0));
            points2.put("p3", new Point(10.0, 100.0));
            List<NamedPoints> namedPoints = Arrays.asList(
                    new NamedPoints("a", points1),
                    new NamedPoints("b", points2)
            );

            Dataset<NamedPoints> namedPointsDS =
                    spark.createDataset(namedPoints, namedPointsEncoder);

            System.out.println("*** here is the schema inferred from the bean");
            namedPointsDS.printSchema();

            System.out.println("*** here is the data");
            namedPointsDS.show();

            System.out.println("*** filter and select using map lookup");
            namedPointsDS
                    .where(size(col("points")).gt(1))
                    .select(col("name"),
                            size(col("points")).as("count"),
                            col("points").getItem("p1")).show();

        }

        spark.stop();
    }
}
