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
    private val produceDelay: Long,
    private val messageFactory: MessageFactory,
    private val partitionSize: Int
) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    private val kafkaProducer: KafkaProducer<String, String>
    private val topic: String

    init {
        val configs = TypedProperties("kafka").also {
            it[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
            it[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        }

        kafkaProducer = KafkaProducer<String, String>(configs)
        topic = topics[producerId % topics.size]
    }

    suspend fun produce() {
        kafkaProducer.use { producer ->
            messageFactory
                .generate("$producerId")
                .map {
                    ProducerRecord<String, String>(
                        topic,
                        Random().nextInt(partitionSize),
                        "${UUID.randomUUID()}",
                        it
                    )
                }
                .forEach {record ->
                    producer.send(record)
                    logger.info("Producer $producerId: $record")
                    delay(produceDelay)
            }
            producer.flush()
        }
    }
}
