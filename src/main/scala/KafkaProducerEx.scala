import java.util.Properties
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{ProducerConfig,ProducerRecord,KafkaProducer}

object KafkaProducerEx {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val envprops = conf.getConfig(args(0))
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,envprops.getString("bootstrap-server"))
    props.put(ProducerConfig.CLIENT_ID_CONFIG,"KafkaProdExample")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String,String](props)
    val data = new ProducerRecord[String,String]("Kafka-Testing","key1","value1")

    producer.send(data)
    producer.close()
  }
}
