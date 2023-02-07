import kotlinx.coroutines.delay
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

class SimpleConsumer(consumerId: Int, private val consumePerSec: Int) {

    private val logger = LoggerFactory.getLogger(this::class.java)

    private val configs = javaClass.classLoader.getResourceAsStream("kafka.properties").use {
        Properties().apply { load(it) }
    }.also {
        it[ConsumerConfig.GROUP_ID_CONFIG] = "group-$consumerId"
        it[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        it[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
    }

    private val kafkaConsumer = KafkaConsumer<String, String>(configs)

    private fun chooseTopic(topics: Array<String>) = listOf(topics.random())

    suspend fun consume(topics: Array<String>) {
        kafkaConsumer.subscribe(chooseTopic(topics))

        while (true) {
            val records = kafkaConsumer.poll(Duration.ofSeconds(1))

            records.forEach { record ->
                logger.info("Consumer: $record")
            }

            delay(5000)
        }
    }
}