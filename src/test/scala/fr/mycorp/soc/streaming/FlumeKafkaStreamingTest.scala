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

class FlumeKafkaStreamingTest extends JUnit3Suite with KafkaServerTestHarness {
  
  val numServers = 2
  val configs =
    for(props <- TestUtils.createBrokerConfigs(numServers, false))
    yield new KafkaConfig(props) {
      override val zkConnect = TestZKUtils.zookeeperConnect
      override val numPartitions = 4
    }
  
  override def setUp() {
    super.setUp()
  }

  override def tearDown() {
    super.tearDown()
  }


	// Setup a Kafka server

	/*val chanCtx  = new Context();
	val channel  = new MemoryChannel();
	channel.setName("simpleHDFSTest-mem-chan");
	channel.configure(chanCtx);
	channel.start();*/

  @Test
  def testKafkaFlume() {
    println("Kafka server with broker " + brokerList)
    println("Kafka server with broker " + zkConnect)
    
    // Creation du topic
    TestUtils.createTopic(zkClient, "bloob", 1, 2, servers)
    
    // On essaye d'envoyer un flux
    var producer = TestUtils.createNewProducer(brokerList)
    val partition = new Integer(0)

    Thread.sleep(60000)
	}


}