# _HiveQL_ Examples

Note: these examples are all dependent on giving the JVM more PermGen space using
the following JVM argument:

    -XX:MaxPermSize=128M

| File                     | What's Illustrated    |
|--------------------------|-----------------------|
| SimpleUDF.scala          | Registering and calling a user-defined function in a Hive query -- simple version. |
| SimpleUDAF.scala         | Registering and calling a user-defined aggregation function in a Hive query -- simple version. See the Java companion class in src/main/java/hiveql/SumLargeSalesUDAF.java |
| LateralViewExplode.scala | Using Hive's LATERAL VIEW feature make non-rectangular data look rectangular. |
| PartitionedTable.scala   | Similar to sql/PartitionedTable.sql but for HiveQL: Creating, updating and querying partitioned tables. |