package fr.mycorp.soc.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.flume.source.avro.AvroFlumeEvent
import org.apache.spark.streaming.flume.SparkFlumeEvent

object FlumeKafkaStreaming {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
        |Usage: DirectKafkaWordCount <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("DirectKafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, SparkFlumeEvent, StringDecoder, AvroFlumeEventDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2).foreach(x => println(x.collect().length))
    /*val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()*/

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
  
}