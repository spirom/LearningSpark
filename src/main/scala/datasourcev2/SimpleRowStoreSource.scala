package datasourcev2

import org.apache.spark.sql.sources.v2.writer.{WriterCommitMessage, DataWriter, DataWriterFactory, DataSourceWriter}
import org.apache.spark.sql.{SaveMode, SparkSession, Row}
import org.apache.spark.sql.sources.v2.{WriteSupport, DataSourceOptions, ReadSupport, DataSourceV2}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.types.StructType


import java.util.{List => JList, Optional}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}



/**
 * A very simple simulation of a database -- a mutable object on the driver.
 * This is a rather extreme over-simplification to keep the complexity of the
 * example under control.
 */
object GlobalData {
  private val data = new ArrayBuffer[Row]
  println("*** creating the initial data")
  for (i <- 0 to 9) {
    data += Row(i, -i)
  }

  def nonEmpty = data.nonEmpty

  def rowCount = data.size

  // for writing
  def clear() : Unit = {
    data.clear()
  }

  // for writing
  def add(rowsToAdd: ListBuffer[Row]) : Unit = {
    data ++= rowsToAdd
  }

  // for reading
  def rowSnapshot : ArrayBuffer[Row] = data.clone()
}

/**
 * Convey a partial commit (resulting from a task level commit) to be
 * incorporated in a job level commit.
 * @param rows The data to be committed.
 */
class PartialCommit(val rows: List[Row]) extends WriterCommitMessage {

}

class SimpleRowStoreSource extends DataSourceV2 with ReadSupport with WriteSupport {

  class Reader extends DataSourceReader {
    override def readSchema(): StructType = new StructType().add("i", "int").add("j", "int")

    /**
     * For simplicity, every RDD returned from this source will have two partitions.
     * @return
     */
    override def createDataReaderFactories(): JList[DataReaderFactory[Row]] = {
      println(s"*** creating data reader with ${GlobalData.rowCount} rows")
      val middle = GlobalData.rowCount / 2
      java.util.Arrays.asList(new SimpleRowStoreReaderFactory(0, middle, GlobalData.rowSnapshot),
        new SimpleRowStoreReaderFactory(middle, GlobalData.rowCount, GlobalData.rowSnapshot))
    }
  }

  class Writer extends DataSourceWriter {
    val rowBuffer = new ListBuffer[Row]()

    override def createWriterFactory(): DataWriterFactory[Row] = {
      val factory = new SimpleRowStoreWriterFactory()
      factory
    }

    override def commit(messages: Array[WriterCommitMessage]): Unit = {
      messages.foreach(message => {
        val partialCommit = message.asInstanceOf[PartialCommit]
        println(s"*** job level partial commit of ${partialCommit.rows.size} rows")
        rowBuffer ++= partialCommit.rows
      })
      GlobalData.add(rowBuffer)
      println(s"*** job level commit of ${GlobalData.rowCount} rows")

    }

    override def abort(messages: Array[WriterCommitMessage]): Unit = {
      println(s"*** job level abort of ${rowBuffer.size} rows")
      // not clear whether to also call abort on the factories???
      rowBuffer.clear()
    }
  }

  override def createReader(options: DataSourceOptions): DataSourceReader = new Reader

  override def createWriter(
                             jobId: String,
                             schema: StructType,
                             mode: SaveMode,
                             options: DataSourceOptions): Optional[DataSourceWriter] = {
    if (mode == SaveMode.ErrorIfExists) {
      if (GlobalData.nonEmpty) {
        throw new RuntimeException("data already exists")
      }
    }
    if (mode == SaveMode.Ignore) {
      if (GlobalData.nonEmpty) {
        return Optional.empty()
      }
    }
    if (mode == SaveMode.Overwrite) {
      GlobalData.clear()
    }
    Optional.of(new Writer)
  }
}

class SimpleRowStoreReaderFactory(start: Int, end: Int, data: ArrayBuffer[Row])
  extends DataReaderFactory[Row]
  with DataReader[Row] {

  private var current = start - 1

  override def createDataReader(): DataReader[Row] = new SimpleRowStoreReaderFactory(start, end, data)

  override def next(): Boolean = {
    current += 1
    current < end
  }

  override def get(): Row = data(current)

  override def close(): Unit = {}
}

class SimpleRowStoreWriterFactory extends DataWriterFactory[Row] {

  override def createDataWriter(partitionId: Int,
                                attemptNumber: Int): DataWriter[Row] = {
    new SimpleRowStoreWriter()
  }
}

/**
 * THe writer maintains a local buffer of uncommitted records, which are
 * returned to the driver on a commit. THis buffer is cleared on a commit or abort.
 */
class SimpleRowStoreWriter extends DataWriter[Row] {

  private val uncommitted = new ListBuffer[Row]()

  override def write(record: Row): Unit = {
    println("*** task level write")
    uncommitted += record
  }

  override def commit(): WriterCommitMessage = {
    println(s"*** task level commit of ${uncommitted.size} rows")
    val committing = new ListBuffer[Row]()
    committing ++= uncommitted
    uncommitted.clear()
    val partialCommit = new PartialCommit(committing.toList)
    partialCommit
  }

  override def abort(): Unit = {
    uncommitted.clear()
  }
}

/**
 * Read the contents of the store, update them and read again. When running
 * this, notice that since the DataFrame returned from the read has two
 * partitions (always with this data source), the write results in two tasks,
 * and hence two task level commits.
 */
object SimpleRowStoreSource {
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("DatasourceV2-SimpleRowStoreSource")
        .master("local[4]")
        .getOrCreate()

    //
    // Set up the data source: it has data by default so see what it is
    //
    val source = "datasourcev2.SimpleRowStoreSource"
    val df = spark.read.format(source).load()
    println("*** Initial contents of data source")
    df.printSchema()
    df.show()

    //
    // Set up another data frame to write to the above data source in
    // various values of SaveMode
    //
    val rowsToWrite = Seq(
      Row(100, -100),
      Row(200, -200),
      Row(300, -300),
      Row(400, -400)
    )
    val rowsToWriteRDD = spark.sparkContext.parallelize(rowsToWrite, 4)
    val dfToWrite = spark.createDataFrame(rowsToWriteRDD, df.schema)

    //
    // SaveMode.ErrorIfExists
    //
    try {
      dfToWrite.write.format(source).mode(SaveMode.ErrorIfExists).save()
      println("*** Write should have failed, but didn't!")
    } catch  {
      case re: RuntimeException => {
        println(s"*** Threw RuntimeException as expected: ${re.getMessage}")
      }
      case e: Exception => {
        println(s"*** Threw unexpected exception: ${e.getMessage}")
      }
    }
    val df1 = spark.read.format(source).load()
    println("*** SaveMode.ErrorIfExists: exception and no change")
    df1.printSchema()
    df1.show()

    //
    // SaveMode.Append
    //
    dfToWrite.write.format(source).mode(SaveMode.Append).save()
    val df2 = spark.read.format(source).load()
    println("*** SaveMode.Append: rows are added")
    df2.printSchema()
    df2.show()

    //
    // SaveMode.Overwrite
    //
    dfToWrite.write.format(source).mode(SaveMode.Overwrite).save()
    val df3 = spark.read.format(source).load()
    println("*** SaveMode.Overwrite: old rows are replaced")
    df3.printSchema()
    df3.show()

    //
    // SaveMode.Ignore
    //
    dfToWrite.write.format(source).mode(SaveMode.Ignore).save()
    val df4 = spark.read.format(source).load()
    println("*** SaveMode.Ignore: no change")
    df4.printSchema()
    df4.show()

    spark.stop()

  }
}