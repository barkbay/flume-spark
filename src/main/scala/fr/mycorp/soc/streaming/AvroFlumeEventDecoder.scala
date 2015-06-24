package fr.mycorp.soc.streaming

import org.apache.flume.source.avro.AvroFlumeEvent
import java.io.ByteArrayInputStream
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.log4j.Logger
import kafka.utils.VerifiableProperties
import org.apache.avro.io.BinaryDecoder
import org.apache.spark.streaming.flume.SparkFlumeEvent
import org.apache.spark.streaming.flume.SparkFlumeEvent

/**
 * @author michael
 */
class AvroFlumeEventDecoder(props: VerifiableProperties = null) extends kafka.serializer.Decoder[SparkFlumeEvent] {
  
  val log = Logger.getLogger("bla")
  
  def fromBytes(bytes: Array[Byte]): SparkFlumeEvent = {
    log.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    val in = new ByteArrayInputStream(bytes)
    val decoder:BinaryDecoder = DecoderFactory.get.directBinaryDecoder(in, null)
    val reader = new SpecificDatumReader[AvroFlumeEvent](classOf[AvroFlumeEvent])
    val data = reader.read(null, decoder)
    log.error("HEADER=" + data.getHeaders)
    log.error("BODY=" + data.getBody)
    val event = new SparkFlumeEvent
    event.event = data
    event
  }
}