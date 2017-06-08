
# Spark Streaming Examples

## Basic

| File      | What's Illustrated |
| --------- | ------------------ |
| [FileBased.scala](FileBased.scala) | Streaming from a sequence of files. |
| [QueueBased.scala](QueueBased.scala) | Streaming from a queue. |
| [Accumulation.scala](Accumulation.scala) | Accumulating stream data in a single RDD. |
| [Windowing.scala](Windowing.scala) | Maintaining a sliding window on the most recent stream data. |

## Advanced

| File      | What's Illustrated |
| --------- | ------------------ |
| [CustomReceiver.scala](CustomReceiver.scala) | Implement a very simple but well behaved custom Receiver. |
| [Monitoring.scala](Monitoring.scala) | Extend StreamListener to monitor the number of records passing through a stream. |

## Structured Streaming

For examples of structured streaming as introduced in Spark 2.0, see the [structured](structured) directory.

## Streaming with Apache Kafka

For a new, separate GitHub project that provides self-contained examples of using
Spark Streaming with Kafka, please look
[here](https://github.com/spirom/spark-streaming-with-kafka).
