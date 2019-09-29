
import  org.apache.kafka.clients.producer.{ProducerConfig,ProducerRecord,KafkaProducer}
import com.typesafe.config.ConfigFactory
import java.util.Properties
import scala.io.Source

object KafkaReadLogs {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val envprops = conf.getConfig(args(0))
    val props = new Properties()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,envprops.getString("bootstrap-server"))
    props.put(ProducerConfig.CLIENT_ID_CONFIG,"KafkalogExample")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[Nothing,String](props)

    val file = Source.fromFile("/opt/gen_logs/logs/access.log").getLines()

    val logs = file.toList

    logs.foreach( msg => { val record = new ProducerRecord("retail_multi",msg)
                              producer.send(record)
    }
    )
    producer.close()
  }
}