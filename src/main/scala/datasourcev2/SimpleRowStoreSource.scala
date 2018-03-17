package datasourcev2

import org.apache.spark.sql.sources.v2.writer.{WriterCommitMessage, DataWriter, DataWriterFactory, DataSourceWriter}
import org.apache.spark.sql.{SaveMode, SparkSession, Row}
import org.apache.spark.sql.sources.v2.{WriteSupport, DataSourceOptions, ReadSupport, DataSourceV2}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.types.StructType


import java.util.{List => JList, Optional, ArrayList}

class SimpleRowStoreSource  extends DataSourceV2 with ReadSupport with WriteSupport {

  class Reader extends DataSourceReader {
    override def readSchema(): StructType = new StructType().add("i", "int").add("j", "int")

    override def createDataReaderFactories(): JList[DataReaderFactory[Row]] = {
      java.util.Arrays.asList(new SimpleRowStoreReaderFactory(0, 5), new SimpleRowStoreReaderFactory(5, 10))
    }
  }

  class Writer extends DataSourceWriter {
    override def createWriterFactory(): DataWriterFactory[Row] = {
      throw new IllegalArgumentException("not expected!")
    }

    override def commit(messages: Array[WriterCommitMessage]): Unit = {

    }

    override def abort(messages: Array[WriterCommitMessage]): Unit = {

    }
  }

  override def createReader(options: DataSourceOptions): DataSourceReader = new Reader

  override def createWriter(
                             jobId: String,
                             schema: StructType,
                             mode: SaveMode,
                             options: DataSourceOptions): Optional[DataSourceWriter] = {
    Optional.of(new Writer)
  }
}

class SimpleRowStoreReaderFactory(start: Int, end: Int)
  extends DataReaderFactory[Row]
  with DataReader[Row] {
  private var current = start - 1

  override def createDataReader(): DataReader[Row] = new SimpleRowStoreReaderFactory(start, end)

  override def next(): Boolean = {
    current += 1
    current < end
  }

  override def get(): Row = Row(current, -current)

  override def close(): Unit = {}
}

class SimpleRowStoreWriterFactory()
  extends DataWriterFactory[Row] {

  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = {
    new SimpleRowStoreWriter
  }
}

class SimpleRowStoreWriter() extends DataWriter[Row] {



  override def write(record: Row): Unit = {

  }

  override def commit(): WriterCommitMessage = {

    null
  }

  override def abort(): Unit = {
    try {

    } finally {

    }
  }
}

object SimpleRowStoreSource {
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("DatasourceV2-SimpleRowStoreSource")
        .master("local[4]")
        .getOrCreate()

    val source = "datasourcev2.SimpleRowStoreSource"

    val df = spark.read.format(source).load()

    df.printSchema()

    df.show()

    df.write.format(source).mode("overwrite").save()

    val df2 = spark.read.format(source).load()

    df2.printSchema()

    df2.show()

    spark.stop()

  }
}