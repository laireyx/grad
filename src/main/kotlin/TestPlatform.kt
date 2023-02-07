import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

class TestPlatform {

    private val topics: Array<String>

    private val consumers: Array<SimpleConsumer>
    private val producers: Array<SimpleProducer>

    init {
        val configs = TypedProperties("test")

        val testerName = configs["tester.name"] ?: "anonymous"

        // Initialize topic names
        topics = Array(configs["topic.size"]) { "topic-$testerName-$it" }

        // Initialize consumers
        consumers = Array(configs["consumer.size"]) {
            SimpleConsumer(
                it,
            )
        }

        // Initialize producers
        producers = Array(configs["producer.size"]) {
            SimpleProducer(
                it,
                configs["message.delay"],
                configs["message.count"],
                configs["message.minLen"],
                configs["message.maxLen"]
            )
        }
    }

    suspend fun test() = runBlocking {
        consumers.forEach {
            launch { it.consume(topics) }
        }
        producers.forEach {
            launch { it.produce(topics) }
        }
    }
}