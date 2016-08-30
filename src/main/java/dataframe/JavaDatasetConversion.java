package dataframe;

import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;

//
// Explore interoperability between DataFrame and Dataset. Note that Dataset
// is covered in much greater detail in the 'dataset' directory.
//
public class JavaDatasetConversion {

    //
    // This must be a JavaBean in order for Spark to infer a schema for it
    //
    public static class Cust implements Serializable {
        private int id;
        private String name;
        private double sales;
        private double discount;
        private String state;

        public Cust(int id, String name, double sales, double discount, String state) {
            this.id = id;
            this.name = name;
            this.sales = sales;
            this.discount = discount;
            this.state = state;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public double getSales() {
            return sales;
        }

        public void setSales(double sales) {
            this.sales = sales;
        }

        public double getDiscount() {
            return discount;
        }

        public void setDiscount(double discount) {
            this.discount = discount;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }
    }

    //
    // A smaller JavaBean for a subset of the fields
    //
    public static class StateSales implements Serializable {
        private double sales;
        private String state;

        public StateSales(int id, String name, double sales, double discount, String state) {
            this.sales = sales;
            this.state = state;
        }

        public double getSales() {
            return sales;
        }

        public void setSales(double sales) {
            this.sales = sales;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
            .builder()
            .appName("DataFrame-Java-DatasetConversion")
            .master("local[4]")
            .getOrCreate();

        //
        // The Java API requires you to explicitly instantiate an encoder for
        // any JavaBean you want to use for schema inference
        //
        Encoder<Cust> custEncoder = Encoders.bean(Cust.class);
        //
        // Create a container of the JavaBean instances
        //
        List<Cust> data = Arrays.asList(
                new Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
                new Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
                new Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
                new Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
                new Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
        );
        //
        // Use the encoder and the container of JavaBean instances to create a
        // Dataset
        //
        Dataset<Cust> ds = spark.createDataset(data, custEncoder);

        System.out.println("*** here is the schema inferred from the Cust bean");
        ds.printSchema();

        System.out.println("*** here is the data");
        ds.show();

        //
        // Querying a Dataset of any type results in a
        // DataFrame (i.e. Dastaset<Row>)
        //

        Dataset<Row> smallerDF =
                ds.select("sales", "state").filter(col("state").equalTo("CA"));

        System.out.println("*** here is the dataframe schema");
        smallerDF.printSchema();

        System.out.println("*** here is the data");
        smallerDF.show();

        //
        // But a Dataset<Row> can be converted back to a Dataset of some other
        // type by using another bean encoder
        //

        Encoder<StateSales> stateSalesEncoder = Encoders.bean(StateSales.class);

        Dataset<StateSales> stateSalesDS = smallerDF.as(stateSalesEncoder);

        System.out.println("*** here is the schema inferred from the StateSales bean");
        stateSalesDS.printSchema();

        System.out.println("*** here is the data");
        stateSalesDS.show();

        spark.stop();
    }
}
