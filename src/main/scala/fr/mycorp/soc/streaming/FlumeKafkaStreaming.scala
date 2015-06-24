package fr.mycorp.soc.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.flume.source.avro.AvroFlumeEvent
import org.apache.spark.streaming.flume.SparkFlumeEvent
import org.apache.log4j.Logger

object FlumeKafkaStreaming {
  
  val logger = Logger.getLogger("fr.mycorp.soc.streaming.FlumeKafkaStreaming")
  
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
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("CoutingCats")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, SparkFlumeEvent, StringDecoder, AvroFlumeEventDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val lines = messages.foreachRDD( rdd => {
      val offsetRanges:HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
      offsetRanges.offsetRanges.foreach { 
        of:OffsetRange => println(Thread.currentThread().getName + " : " + of.topic + "/" + of.partition + "/" + of.fromOffset + "=>" + of.untilOffset)
        }
    	println(Thread.currentThread().getName + "RECEIVED : " + rdd.collect().length)      
    })
      


    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
  
}