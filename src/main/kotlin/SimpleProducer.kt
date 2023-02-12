import kotlinx.coroutines.delay
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.DurationUnit

class SimpleProducer(
    private val producerId: Int,
    topics: Array<String>,
    private val producePerSec: Int,
    private val produceCount: Int,
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
        var remainingCount = produceCount
        var producePerStep = producePerSec
        val startTime = System.nanoTime()

        kafkaProducer.use { producer ->
            while(remainingCount > 0) {

                // Create {produceCount} messages in each step
                remainingCount -= producePerStep

                messageFactory
                    .generate("$producerId", producePerStep)
                    .map {
                        ProducerRecord<String, String>(
                            topic,
                            Random().nextInt(partitionSize),
                            "${UUID.randomUUID()}",
                            it
                        )
                    }
                    .forEach { record ->
                        producer.send(record)
                    }
                producer.flush()

                val duration = (System.nanoTime() - startTime).nanoseconds
                val durationInMs = duration.toLong(DurationUnit.MILLISECONDS)

                val itShouldTake = 1000 * (produceCount - remainingCount) / producePerSec

                if (durationInMs < itShouldTake) {
                    delay(itShouldTake - durationInMs)
                    producePerStep = (producePerStep / 2).coerceAtLeast(1)
                } else {
                    if(durationInMs > itShouldTake * 2) {
                        producePerStep *= 2
                    } else {
                        producePerStep += (producePerStep / 10).coerceAtLeast(1)
                    }
                }

                logger.info("Producer $producerId: produce [${produceCount - remainingCount}/$produceCount] messages in [$durationInMs] ms.")
                delay(1)
            }
        }
    }
}
