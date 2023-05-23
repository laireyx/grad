package test

import event.SimpleEvent
import kotlinx.coroutines.delay
import org.slf4j.LoggerFactory
import utils.PropertyHolder
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.DurationUnit

class TestScenario(scenarioType: String) : PropertyHolder("scenarios", scenarioType) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    private val scenarioName: String = configs["scenario.name"]

    private val eventNumber: Int = configs["event.number"]
    private val eventTypes: Array<String> = configs["event.type[]"]

    private val eventsPerSec: Int = configs["event.persec"]

    private var remainingCount = eventNumber
    private var producePerStep = eventsPerSec

    // Start time of this scenario
    private val startTime = System.nanoTime()

    val isEnd: Boolean
        get() {
            return remainingCount < 0
        }

    suspend fun nextEvent(): SimpleEvent {
        adjustThroughput()
        // Create {produceCount} messages in each step
        remainingCount -= producePerStep

        return SimpleEvent(eventTypes.random())
    }

    private suspend fun adjustThroughput() {
        val duration = (System.nanoTime() - startTime).nanoseconds
        val durationInMs = duration.toLong(DurationUnit.MILLISECONDS)

        val itShouldTake = 1000 * (eventNumber - remainingCount) / eventsPerSec

        if (durationInMs < itShouldTake) {
            delay(itShouldTake - durationInMs)
            producePerStep = (producePerStep / 2).coerceAtLeast(1)
        } else {
            if (durationInMs > itShouldTake * 2) {
                producePerStep *= 2
            } else {
                producePerStep += (producePerStep / 10).coerceAtLeast(1)
            }
        }

        logger.info("Scenario $scenarioName: fired [${eventNumber - remainingCount}/$eventNumber] events in [$durationInMs] ms.")
    }
}