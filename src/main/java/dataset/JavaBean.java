package dataset;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple3;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;

//
// Create a Spark Dataset from an array of JavaBean instances.
// The inferred schema has convenient column names and it can
// be queried conveniently.
//
public class JavaBean {

    //
    // This must be a JavaBean in order for Spark to infer a schema for it
    //
    public static class Number implements Serializable {
        private int i;
        private String english;
        private String french;

        public Number(int i, String english, String french) {
            this.i = i;
            this.english = english;
            this.french = french;
        }

        public int getI() {
            return i;
        }

        public void setI(int i) {
            this.i = i;
        }

        public String getEnglish() {
            return english;
        }

        public void setEnglish(String english) {
            this.english = english;
        }

        public String getFrench() {
            return french;
        }

        public void setFrench(String french) {
            this.french = french;
        }
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
            .builder()
            .appName("Dataset-Java-Bean")
            .master("local[4]")
            .getOrCreate();

        //
        // The Java API requires you to explicitly instantiate an encoder for
        // any JavaBean you want to use for schema inference
        //
        Encoder<Number> numberEncoder = Encoders.bean(Number.class);
        //
        // Create a container of the JavaBean instances
        //
        List<Number> data = Arrays.asList(
                new Number(1, "one", "un"),
                new Number(2, "two", "deux"),
                new Number(3, "three", "trois"));
        //
        // Use the encoder and the container of JavaBean instances to create a
        // Dataset
        //
        Dataset<Number> ds = spark.createDataset(data, numberEncoder);

        System.out.println("*** here is the schema inferred from the bean");
        ds.printSchema();

        System.out.println("*** here is the data");
        ds.show();

        // Use the convenient bean-inferred column names to query
        System.out.println("*** filter by one column and fetch others");
        ds.where(col("i").gt(2)).select(col("english"), col("french")).show();

        spark.stop();
    }
}
