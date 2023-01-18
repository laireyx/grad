
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*

class SimpleProducer {
    private val logger = LoggerFactory.getLogger(this::class.java)

    // Topic name
    private val TOPIC_NAME = "test"

    fun testSimpleProducer() {
        val configs = javaClass.classLoader.getResourceAsStream("kafka.properties").use {
            Properties().apply { load(it) }
        }
        // Serialize / Deserialize logic
        configs[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        configs[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name

        KafkaProducer<String, String>(configs).use { producer ->
            val messageValue = "testMessage"
            val record = ProducerRecord<String, String>(TOPIC_NAME, messageValue)

            while(true) {
                producer.send(record)
                logger.info("Producer: $record")
                producer.flush()

                Thread.sleep(1000)
            }
        }
    }
}
