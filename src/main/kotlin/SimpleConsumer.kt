import kotlinx.coroutines.delay
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration

class SimpleConsumer(
    private val consumerId: Int,
    private val maximumInterval: Long,
    private val retryIfEmpty: Int,
    topics: Array<String>
) {

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

        var interval: Long = 100
        var retryCnt = 0

        while (retryCnt != retryIfEmpty) {
            kafkaConsumer.poll(Duration.ofMillis(interval)).let { records ->
                if (records.isEmpty) {
                    if (interval == maximumInterval) {
                        retryCnt++
                    } else {
                        interval = (interval * 2).coerceAtMost(maximumInterval)
                    }
                } else {
                    retryCnt = 0
                    interval = (interval / 2).coerceAtLeast(100)
                }

                logger.info("Consumer $consumerId: polling [$interval]ms and got [${records.count()}] records.")
            }

            delay(interval)
        }
    }
}