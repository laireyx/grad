import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import test.TestScenario

class SimpleProducer(
    producerId: Int,
    topics: Array<String>,
    private val partitionSize: Int
) {

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

    suspend fun runScenario() {
        val scenario = TestScenario("basic")

        scenario.run(kafkaProducer, topic, partitionSize)
    }
}
