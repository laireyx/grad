import kotlinx.coroutines.delay
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*

class SimpleProducer(
    private val producerId: Int,
    topics: Array<String>,
    private val producePerMillis: Long,
    private val messageCount: Int,
    private val minLen: Int,
    private val maxLen: Int
) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    private val configs = TypedProperties("kafka").also {
        it[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        it[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
    }

    private val kafkaProducer = KafkaProducer<String, String>(configs)

    private val topic: String = topics[producerId % topics.size]

    private fun generateMessage(): String {
        val length = Random().nextInt(minLen, maxLen + 1)
        return "[$producerId]".padEnd(length, '@')
    }

    suspend fun produce() {
        kafkaProducer.use { producer ->
            repeat(messageCount) {
                val record = ProducerRecord<String, String>(topic, generateMessage())
                producer.send(record)
                logger.info("Producer $producerId: $record")
                delay(producePerMillis)
            }
            producer.flush()
        }
    }
}
