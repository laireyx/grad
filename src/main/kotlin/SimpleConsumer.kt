import kotlinx.coroutines.delay
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

class SimpleConsumer {

    private val logger = LoggerFactory.getLogger(this::class.java)

    private val TOPIC_NAME = "test"
    private val GROUP_ID = "test-group"

    suspend fun testConsumer() {
        val configs = javaClass.classLoader.getResourceAsStream("kafka.properties").use {
            Properties().apply { load(it) }
        }
        configs[ConsumerConfig.GROUP_ID_CONFIG] = GROUP_ID
        configs[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        configs[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name

        val consumer = KafkaConsumer<String, String>(configs)
        consumer.subscribe(listOf(TOPIC_NAME))

        while (true) {
            val records = consumer.poll(Duration.ofSeconds(1))

            records.forEach { record ->
                logger.info("Consumer: $record")
            }

            delay(5000)
        }
    }
}