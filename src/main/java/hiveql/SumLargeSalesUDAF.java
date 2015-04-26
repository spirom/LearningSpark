package hiveql;

//
// A Hive user-defined aggregation function of the simple variety.
// Sums doubles whose value is > 500.
//
// Note: this approach to defining a Hive UDAF (ie: extending the UDAF class) has long
// been deprecated,
//
public class SumLargeSalesUDAF extends org.apache.hadoop.hive.ql.exec.UDAF {

    static Double state = new Double(0);

    public static class Evaluator implements org.apache.hadoop.hive.ql.exec.UDAFEvaluator {
        public void init() {
            state = new Double(0);
        }

        public boolean iterate(Double s) {
            if (s != null && s > 500) {
                state = state + s;
            }
            return true;
        }

        public Double terminatePartial() {
            return state;
        }

        public boolean merge(Double s) {
            if (s != null) {
                state = state + s;
            }
            return true;
        }

        public Double terminate() {
            return state;
        }

    }

}
