import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

class TestPlatform {

    private val topics: Array<String>

    private val consumers: Array<SimpleConsumer>
    private val producers: Array<SimpleProducer>

    init {
        val configs = TypedProperties("test")

        val testerName = configs["tester.name"] ?: "anonymous"
        val messageFactory = MessageFactory(
            configs["message.batchSize"],
            configs["message.minLen"],
            configs["message.maxLen"]
        )

        // Initialize topic names
        topics = Array(configs["topic.size"]) { "topic-$testerName-$it" }

        // Initialize consumers
        consumers = Array(configs["consumer.size"]) {
            SimpleConsumer(
                it,
                topics
            )
        }

        // Initialize producers
        producers = Array(configs["producer.size"]) {
            SimpleProducer(
                it,
                topics,
                configs["producer.produceDelay"],
                messageFactory,
                configs["partition.size"]
            )
        }
    }

    suspend fun test() = runBlocking {
        consumers.forEach {
            launch { it.consume() }
        }
        producers.forEach {
            launch { it.produce() }
        }
    }
}