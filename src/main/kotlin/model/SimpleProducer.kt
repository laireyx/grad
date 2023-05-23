package model

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import test.TestScenario
import utils.PropertyHolder
import java.util.*

class SimpleProducer(
    producerId: Int,
    topics: Array<String>,
    private val partitionSize: Int
) : PropertyHolder("kafka") {

    private val kafkaProducer: KafkaProducer<String, String>
    private val topic: String

    init {
        val kafkaConfigs = configs.also {
            it[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
            it[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        }

        kafkaProducer = KafkaProducer<String, String>(kafkaConfigs)
        topic = topics[producerId % topics.size]
    }

    suspend fun runScenario(scenario: TestScenario) {
        while (!scenario.isEnd) {
            val event = scenario.nextEvent()
            val firedMessages = event.fireMessages()

            firedMessages.asSequence().map {
                ProducerRecord(
                    topic,
                    Random().nextInt(partitionSize),
                    "${UUID.randomUUID()}",
                    it.body
                )
            }.forEach {
                kafkaProducer.send(it)
            }

            kafkaProducer.flush()
        }
    }
}
