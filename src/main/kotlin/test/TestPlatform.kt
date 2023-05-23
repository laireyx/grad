package test

import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import model.SimpleConsumer
import model.SimpleProducer
import utils.PropertyHolder

class TestPlatform : PropertyHolder("test") {

    private val topics: Array<String>

    private val consumers: Array<SimpleConsumer>
    private val producers: Array<SimpleProducer>

    init {
        val testerName = configs["tester.name"] ?: "anonymous"

        // Initialize topic names
        topics = Array(configs["topic.size"]) { "topic-$testerName-$it" }

        // Initialize consumers
        consumers = Array(configs["consumer.size"]) {
            SimpleConsumer(
                it,
                configs["consumer.maximumInterval"],
                configs["consumer.retryIfEmpty"],
                topics
            )
        }

        // Initialize producers
        producers = Array(configs["producer.size"]) {
            SimpleProducer(
                it,
                topics,
                configs["partition.size"]
            )
        }
    }

    suspend fun test() = runBlocking {
        consumers.forEach {
            launch { it.consume() }
        }
        producers.forEach {
            launch { testProducer(it) }
        }
    }

    private suspend fun testProducer(producer: SimpleProducer, scenarioType: String = "basic") = runBlocking {
        val testScenario = TestScenario(scenarioType)
        producer.runScenario(testScenario)
    }
}