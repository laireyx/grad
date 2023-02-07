import kotlinx.coroutines.delay
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration

class SimpleConsumer(private val consumerId: Int, topics: Array<String>) {

    private val logger = LoggerFactory.getLogger(this::class.java)

    private val kafkaConsumer: KafkaConsumer<String, String>
    private val topic: List<String>

    init {
        val configs = TypedProperties("kafka").also {
            it[ConsumerConfig.GROUP_ID_CONFIG] = "group-${System.currentTimeMillis()}"
            it[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
            it[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        }

        kafkaConsumer = KafkaConsumer<String, String>(configs)
        topic = listOf(topics[consumerId % topics.size])
    }

    suspend fun consume() {
        kafkaConsumer.subscribe(topic)

        while (true) {
            kafkaConsumer.poll(Duration.ofMillis(100)).let { records ->
                if (records.isEmpty) {
                    logger.info("Record empty")
                }

                records.forEach { record ->
                    logger.info("Consumer $consumerId: $record")
                }
            }

            delay(100)
        }
    }
}