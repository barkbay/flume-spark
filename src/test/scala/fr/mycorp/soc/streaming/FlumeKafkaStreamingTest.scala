package fr.mycorp.soc.streaming

import java.util.Properties
import java.util.concurrent.TimeUnit
import kafka.consumer.SimpleConsumer
import kafka.integration.KafkaServerTestHarness
import kafka.message.Message
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.junit.Assert._
import org.junit.Test
import org.scalatest.junit.JUnit3Suite
import kafka.utils.TestZKUtils
import org.apache.flume.source.SyslogTcpSource
import org.apache.flume.channel.kafka.KafkaChannel
import org.apache.flume.Context
import org.apache.flume.channel.kafka.KafkaChannelConfiguration
import org.apache.flume.conf.Configurables
import org.apache.flume.channel.ReplicatingChannelSelector
import scala.collection.JavaConverters._
import org.apache.flume.Channel
import org.apache.flume.channel.ChannelProcessor
import org.joda.time.DateTime
import java.net.Socket

class FlumeKafkaStreamingTest extends JUnit3Suite with KafkaServerTestHarness {
  
  val numServers = 2
  val configs =
    for(props <- TestUtils.createBrokerConfigs(numServers, false))
    yield new KafkaConfig(props) {
      override val zkConnect = TestZKUtils.zookeeperConnect
      override val numPartitions = 4
    }

  @Test
  def testKafkaFlume() {
    
    println("Kafka server with broker " + brokerList)
    println("Zookeeper server is on " + zkConnect)
    
    // Create Kafka topic
    TestUtils.createTopic(zkClient, "bloob", 4, 2, servers)
    
    // Create Kafka Channel
    val channel:Channel = new KafkaChannel
    val context = new Context();
    context.put(KafkaChannelConfiguration.BROKER_LIST_FLUME_KEY, brokerList);
    context.put(KafkaChannelConfiguration.ZOOKEEPER_CONNECT_FLUME_KEY, zkConnect);
    context.put(KafkaChannelConfiguration.PARSE_AS_FLUME_EVENT, "true");
    context.put(KafkaChannelConfiguration.READ_SMALLEST_OFFSET, "true");
    context.put(KafkaChannelConfiguration.TOPIC, "bloob");
    Configurables.configure(channel, context);
    channel.start();
    
    // Create Flume channel selector
    val channels = List(channel).asJava;
    val rcs = new ReplicatingChannelSelector;
    rcs.setChannels(channels);
    
    // Create syslog source
    val source = new SyslogTcpSource
    source.setChannelProcessor(new ChannelProcessor(rcs));
    val sourceContext = new Context();
    sourceContext.put("port", String.valueOf(9999));
    sourceContext.put("keepFields", "true");
    source.configure(sourceContext); 
    source.start();

    // Start Spark Streaming
    FlumeKafkaStreaming.main(Array(brokerList, "bloob"))
    
    Thread.sleep(600000)
	}
  
  def createEvent() = {
    val time = new DateTime()
    val stamp1 = time.toString()
    val host1 = "localhost.localdomain"
    val data1 = "test syslog data"
    val bodyWithHostname = host1 + " " + data1
    val bodyWithTimestamp = stamp1 + " " + data1
    val body = "<10>" + stamp1 + " " + host1 + " " + data1 + "\n"
    
    val syslogSocket = new Socket("localhost", 9999)
    syslogSocket.getOutputStream().write(body.getBytes())
    syslogSocket.close()
  }


}