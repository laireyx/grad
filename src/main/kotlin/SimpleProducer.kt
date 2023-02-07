import kotlinx.coroutines.delay
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*

class SimpleProducer(
    private val producerId: Int,
    private val producePerMillis: Long,
    private val minLen: Int,
    private val maxLen: Int
) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    private val configs = javaClass.classLoader.getResourceAsStream("kafka.properties").use {
        Properties().apply { load(it) }
    }.also {
        it[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        it[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
    }

    private val kafkaProducer = KafkaProducer<String, String>(configs)

    private fun chooseTopic(topics: Array<String>) = topics.random()
    private suspend fun generateMessage(): String {
        delay(producePerMillis)
        val length = Random().nextInt(minLen, maxLen + 1)
        return "[$producerId]".padEnd(length, '@')
    }

    suspend fun produce(topics: Array<String>) {
        kafkaProducer.use { producer ->
            repeat(10) {
                val record = ProducerRecord<String, String>(chooseTopic(topics), generateMessage())
                producer.send(record)
                logger.info("Producer: $record")
                producer.flush()
            }
        }
    }
}
