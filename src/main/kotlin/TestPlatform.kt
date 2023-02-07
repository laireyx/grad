import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.*

class TestPlatform() {
    private val configs = javaClass.classLoader.getResourceAsStream("test.properties").use {
        Properties().apply { load(it) }
    }

    private val topics: Array<String> = Array(configs["topic.size"].toString().toInt()) { "topic-$it" }

    private val consumers: Array<SimpleConsumer> = Array(configs["consumer.size"].toString().toInt()) {
        SimpleConsumer(
            it,
            configs["consumer.consumepersec"].toString().toInt()
        )
    }
    private val producers: Array<SimpleProducer> = Array(configs["producer.size"].toString().toInt()) {
        SimpleProducer(
            it,
            configs["producer.producepersec"].toString().toInt()
        )
    }

    init {
        // TODO: initialize something
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