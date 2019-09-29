import java.util.Collections
import scala.collection.JavaConverters._
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import java.util.Properties

object KafkaConsumerEx {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load
    val envprop = config.getConfig(args(0))
    val props = new Properties
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,envprop.getString("bootstrap-server"))
    props.put(ConsumerConfig.CLIENT_ID_CONFIG,"Kafka-Consumer")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.GROUP_ID_CONFIG,"grp-test")

    val consumer = new KafkaConsumer[String,String](props)
    consumer.subscribe(Collections.singletonList("Kafka-Testing"))
    while(true){
      val records = consumer.poll(500).asScala
      for(record <-records.iterator){
        println("Retrieve message("+ record.key() + record.value() +" - at offset "+ record.offset()+")")
      }
    }
  }
}
