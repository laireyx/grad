package test

import TypedProperties
import event.SimpleEvent
import kotlinx.coroutines.delay
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import java.util.*
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.DurationUnit

class TestScenario(scenarioType: String) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    private val configs = TypedProperties("scenarios/$scenarioType")

    private val scenarioName: String = configs["scenario.name"]

    private val eventNumber: Int = configs["event.number"]
    private val eventTypes: Array<String> = configs["event.type[]"]

    private val eventsPerSec: Int = configs["event.persec"]

    suspend fun run(kafkaProducer: KafkaProducer<String, String>, topic: String, partitionSize: Int) {
        var remainingCount = eventNumber
        var producePerStep = eventsPerSec
        val startTime = System.nanoTime()

        kafkaProducer.use { producer ->
            while(remainingCount > 0) {

                // Create {produceCount} messages in each step
                remainingCount -= producePerStep

                val event = SimpleEvent(eventTypes.random())
                event.fire()
                    .map {
                        ProducerRecord(
                            topic,
                            Random().nextInt(partitionSize),
                            "${UUID.randomUUID()}",
                            it.body
                        )
                    }
                    .forEach { record ->
                        producer.send(record)
                    }
                producer.flush()

                val duration = (System.nanoTime() - startTime).nanoseconds
                val durationInMs = duration.toLong(DurationUnit.MILLISECONDS)

                val itShouldTake = 1000 * (eventNumber - remainingCount) / eventsPerSec

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

                logger.info("Scenario $scenarioName: fired [${eventNumber - remainingCount}/$eventNumber] events in [$durationInMs] ms.")
                delay(1)
            }
        }
    }
}